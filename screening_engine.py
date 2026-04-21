#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Moteur de screening GDA/PPE + Chargement dynamique des sanctions (DG Trésor / FATF)

Sources GDA :
  - API France (DG Trésor) — source primaire
  - API Monaco               — source de repli automatique

Sources sanctions pays :
  - DG Trésor sanctions géographiques (lien permanent, scraping HTML)
  - FATF liste noire + liste grise (URL auto-détectée selon calendrier des plénières)
  Cache TTL 24h avec fallback statique.
"""

import requests
import unicodedata
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
from rapidfuzz import fuzz, process
from bs4 import BeautifulSoup

# ── API FRANCE (source primaire) ────────────────────────────────────────────
# Endpoint correct : "flux-json" (et non "fichier-json")
FRANCE_JSON_URL  = "https://gels-avoirs.dgtresor.gouv.fr/ApiPublic/api/v1/publication/derniere-publication-flux-json"
FRANCE_DATE_URL  = "https://gels-avoirs.dgtresor.gouv.fr/ApiPublic/api/v1/publication/derniere-publication-date"

# ── API MONACO (source de repli) ────────────────────────────────────────────
MONACO_JSON_URL  = "https://geldefonds.gouv.mc/directdownload/sanctions.json"
MONACO_DATE_URL  = "https://geldefonds.gouv.mc/api/derniere-publication-date"

# ── Timeouts ─────────────────────────────────────────────────────────────────
FRANCE_TIMEOUT   = 30   # secondes avant de basculer sur Monaco
MONACO_TIMEOUT   = 30   # timeout Monaco (source de secours, plus tolérant)

# ── Seuils de décision ───────────────────────────────────────────────────────
SEUIL_BLOCK  = 85
SEUIL_REVIEW = 75

# PPE Keywords - Liste complète
PPE_KEYWORDS = {
    # Niveau 1 - Très haut risque (score 25)
    # "président": ["président", "president", "présidente"],
    "ministre": ["ministre", "minister", "secretary"],
    "premier_ministre": ["premier ministre", "prime minister", "chef du gouvernement"],

    # Niveau 2 - Haut risque (score 20)
    "député": ["député", "députée", "depute", "deputy", "mp", "membre du parlement"],
    "sénateur": ["sénateur", "sénatrice", "senator"],
    "ambassadeur": ["ambassadeur", "ambassadrice", "ambassador"],
    "général": ["général", "générale", "general"],
    "juge": ["juge", "judge", "magistrat", "magistrate"],
    "procureur": ["procureur", "procureure", "prosecutor", "procurator"],

    # Niveau 3 - Risque modéré (score 15)
    "maire": ["maire", "mayor", "mairesse"],
    "préfet": ["préfet", "préfète", "prefect"],
    "gouverneur": ["gouverneur", "gouverneure", "governor"],
    "consul": ["consul", "consule"],
    "colonel": ["colonel", "colonelle"],
    "commissaire": ["commissaire", "commissioner"],
    "directeur_agence": ["directeur d'agence", "director of agency", "chef d'agence"],
    "président_conseil": ["président du conseil", "council president", "président conseil régional"],

    # Niveau 4 - Risque surveillé (score 10)
    "conseiller": ["conseiller", "conseillère", "councillor", "councilor", "advisor"],
    "adjoint_maire": ["adjoint au maire", "deputy mayor", "adjoint"],
    "chef_cabinet": ["chef de cabinet", "chief of staff"],
    "secrétaire_état": ["secrétaire d'état", "secretary of state"],
    "sous_préfet": ["sous-préfet", "sous-préfète", "sub-prefect"],
}

# ── Pays à risque - Structure détaillée ─────────────────────────────────────
# Chaque pays : variations de nationalité + métadonnées de risque
# risk_level : "LISTE_NOIRE" | "SANCTIONS_UE" | "LISTE_GRISE"
# action : "CONTRE_MESURES" | "VIGILANCE_RENFORCEE" | "DDR_RENFORCEE"
# source : référence réglementaire applicable
# rationale : motif synthétique affiché dans l'alerte

SENSITIVE_NATIONALITIES = {
    # ── LISTE NOIRE FATF (appel à contre-mesures) ───────────────────────────
    "coree_nord": {
        "variations": ["nord-coréen", "nord-coréenne", "north korean", "coree du nord", "corée du nord", "dprk", "rpdc"],
        "risk_level": "LISTE_NOIRE",
        "label": "Corée du Nord (RPDC)",
        "action": "CONTRE_MESURES",
        "source": "FATF – Appel à l'action (oct. 2025) | Résol. CSNU 2270",
        "rationale": "Défaillances stratégiques graves LBC/FT/FP ; risques élevés de financement de la prolifération nucléaire.",
        "fatf_date": "2025-10",
    },
    "iran": {
        "variations": ["iranien", "iranienne", "iran", "iranian"],
        "risk_level": "LISTE_NOIRE",
        "label": "Iran",
        "action": "CONTRE_MESURES",
        "source": "FATF – Appel à l'action (oct. 2025) | DG Trésor – Sanctions nucléaires",
        "rationale": "Plan d'action non résolu depuis 2016 ; risques de financement de prolifération. Obligations découlant des résolutions CSNU sur le nucléaire iranien.",
        "fatf_date": "2025-10",
    },
    "myanmar": {
        "variations": ["birman", "birmane", "myanmar", "burmese", "birmanie"],
        "risk_level": "LISTE_NOIRE",
        "label": "Myanmar (Birmanie)",
        "action": "CONTRE_MESURES",
        "source": "FATF – Appel à l'action (oct. 2025) | DG Trésor – Sanctions",
        "rationale": "Absence persistante de progrès sur le plan d'action FATF ; vigilance renforcée obligatoire sur toutes les transactions.",
        "fatf_date": "2025-10",
    },

    # ── SANCTIONS UE / DG TRÉSOR (embargo, gel d'avoirs) ───────────────────
    "russie": {
        "variations": ["russe", "russia", "russian", "russie"],
        "risk_level": "SANCTIONS_UE",
        "label": "Russie",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°833/2014 et suivants | DG Trésor – Sanctions Russie",
        "rationale": "Embargo et sanctions économiques UE en lien avec l'agression contre l'Ukraine. Gel d'avoirs de nombreuses personnes physiques et entités.",
        "fatf_date": "2022-03",
    },
    "bielorussie": {
        "variations": ["bielorusse", "belarus", "belarusian", "biélorussie", "bielorussie"],
        "risk_level": "SANCTIONS_UE",
        "label": "Biélorussie",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°765/2006 et suivants | DG Trésor – Sanctions Biélorussie",
        "rationale": "Sanctions UE liées aux violations des droits de l'Homme et au soutien à l'agression russe contre l'Ukraine.",
        "fatf_date": "2020-06",
    },
    "syrie": {
        "variations": ["syrien", "syrienne", "syria", "syrian", "syrie"],
        "risk_level": "SANCTIONS_UE",
        "label": "Syrie",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°36/2012 | DG Trésor – Sanctions Syrie | FATF liste grise",
        "rationale": "Embargo et sanctions UE. Pays également sous surveillance renforcée FATF pour défaillances LBC/FT.",
        "fatf_date": "2024-06",
    },
    "libye": {
        "variations": ["libyen", "libyenne", "libyan", "libya", "libye"],
        "risk_level": "SANCTIONS_UE",
        "label": "Libye",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°204/2011 | DG Trésor – Sanctions Libye",
        "rationale": "Embargo sur les armes et sanctions UE. Instabilité politique et risques élevés de blanchiment.",
        "fatf_date": "2011-03",
    },
    "soudan": {
        "variations": ["soudanais", "soudanaise", "sudanese", "sudan", "soudan"],
        "risk_level": "SANCTIONS_UE",
        "label": "Soudan",
        "action": "DDR_RENFORCEE",
        "source": "DG Trésor – Sanctions Soudan | Résol. CSNU 1556",
        "rationale": "Sanctions internationales ; risques élevés BC/FT liés à l'instabilité et aux conflits armés.",
        "fatf_date": "2004-07",
    },
    "zimbabwe": {
        "variations": ["zimbabwéen", "zimbabwéenne", "zimbabwean", "zimbabwe"],
        "risk_level": "SANCTIONS_UE",
        "label": "Zimbabwe",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°314/2004 | DG Trésor – Sanctions Zimbabwe",
        "rationale": "Sanctions UE liées à des violations des droits de l'Homme. Gel d'avoirs de personnes désignées.",
        "fatf_date": "2004-02",
    },
    "nicaragua": {
        "variations": ["nicaraguayen", "nicaraguayenne", "nicaraguan", "nicaragua"],
        "risk_level": "SANCTIONS_UE",
        "label": "Nicaragua",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°2022/1304 | DG Trésor – Sanctions Nicaragua",
        "rationale": "Sanctions UE liées aux violations des droits de l'Homme et à la répression politique.",
        "fatf_date": "2022-07",
    },
    "somalie": {
        "variations": ["somalien", "somalienne", "somali", "somalian", "somalie"],
        "risk_level": "SANCTIONS_UE",
        "label": "Somalie",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°356/2010 | DG Trésor – Sanctions Somalie | Résol. CSNU 1844",
        "rationale": "Embargo et sanctions liés à la piraterie et à Al-Shabaab. Risques élevés de financement du terrorisme.",
        "fatf_date": "2010-04",
    },
    "rca": {
        "variations": ["centrafricain", "centrafricaine", "central african", "république centrafricaine", "rca"],
        "risk_level": "SANCTIONS_UE",
        "label": "République centrafricaine",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°224/2014 | DG Trésor – Sanctions RCA | Résol. CSNU 2127",
        "rationale": "Embargo sur les armes et sanctions liés au conflit armé. Risques élevés BC/FT.",
        "fatf_date": "2013-12",
    },

    # ── LISTE GRISE FATF (surveillance renforcée, fév. 2025) ────────────────
    "congo_rdc": {
        "variations": ["congolais", "congolaise", "congolese", "république démocratique du congo", "rdc", "rd congo"],
        "risk_level": "LISTE_GRISE",
        "label": "République démocratique du Congo",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (fév. 2025)",
        "rationale": "Défaillances stratégiques LBC/FT identifiées par le FATF. Diligence renforcée obligatoire.",
        "fatf_date": "2022-06",
    },
    "mali": {
        "variations": ["malien", "malienne", "malian", "mali"],
        "risk_level": "LISTE_GRISE",
        "label": "Mali",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (fév. 2025)",
        "rationale": "Plan d'action FATF expiré avec travail restant. Défaillances sur le financement du terrorisme.",
        "fatf_date": "2021-06",
    },
    "haiti": {
        "variations": ["haïtien", "haïtienne", "haitian", "haiti", "haïti"],
        "risk_level": "LISTE_GRISE",
        "label": "Haïti",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (fév. 2025)",
        "rationale": "Défaillances stratégiques LBC/FT. Instabilité politique aggravant les risques.",
        "fatf_date": "2020-10",
    },
    "liban": {
        "variations": ["libanais", "libanaise", "lebanese", "lebanon", "liban"],
        "risk_level": "LISTE_GRISE",
        "label": "Liban",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (oct. 2024)",
        "rationale": "Inscrit sur liste grise FATF en oct. 2024 suite à défaillances LBC/FT identifiées.",
        "fatf_date": "2024-10",
    },
    "venezuela": {
        "variations": ["vénézuélien", "vénézuélienne", "venezuelan", "venezuela"],
        "risk_level": "LISTE_GRISE",
        "label": "Venezuela",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (juin 2024)",
        "rationale": "Inscrit liste grise FATF juin 2024 : risques BC liés à l'économie informelle et liens Iran/Caracas.",
        "fatf_date": "2024-06",
    },
    "yemen": {
        "variations": ["yéménite", "yemeni", "yemen", "yémen"],
        "risk_level": "LISTE_GRISE",
        "label": "Yémen",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée | DG Trésor – Sanctions",
        "rationale": "Sous surveillance FATF ; conflit armé et risques élevés de financement du terrorisme.",
        "fatf_date": "2020-10",
    },
    "kenya": {
        "variations": ["kenyan", "kenyane", "kenyan", "kenya"],
        "risk_level": "LISTE_GRISE",
        "label": "Kenya",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (fév. 2024)",
        "rationale": "Inscrit liste grise FATF fév. 2024 ; défaillances sur la supervision des risques BC/FT.",
        "fatf_date": "2024-02",
    },
    "algerie": {
        "variations": ["algérien", "algérienne", "algerian", "algerie", "algérie"],
        "risk_level": "LISTE_GRISE",
        "label": "Algérie",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (oct. 2024)",
        "rationale": "Inscrit liste grise FATF oct. 2024 ; défaillances stratégiques LBC/FT identifiées.",
        "fatf_date": "2024-10",
    },
    "angola": {
        "variations": ["angolais", "angolaise", "angolan", "angola"],
        "risk_level": "LISTE_GRISE",
        "label": "Angola",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (oct. 2024)",
        "rationale": "Inscrit liste grise FATF oct. 2024 ; lacunes en matière de supervision et poursuites BC.",
        "fatf_date": "2024-10",
    },
    "cote_ivoire": {
        "variations": ["ivoirien", "ivoirienne", "ivorian", "cote d'ivoire", "côte d'ivoire"],
        "risk_level": "LISTE_GRISE",
        "label": "Côte d'Ivoire",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (oct. 2024)",
        "rationale": "Inscrit liste grise FATF oct. 2024 malgré progrès notables post-REM 2023.",
        "fatf_date": "2024-10",
    },
    "laos": {
        "variations": ["laotien", "laotienne", "lao", "laos"],
        "risk_level": "LISTE_GRISE",
        "label": "Laos (RDP Lao)",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (fév. 2025)",
        "rationale": "Inscrit liste grise fév. 2025 ; défis persistants sur l'évaluation des risques et la supervision réglementaire.",
        "fatf_date": "2025-02",
    },
    "nepal": {
        "variations": ["népalais", "népalaise", "nepalese", "nepal", "népal"],
        "risk_level": "LISTE_GRISE",
        "label": "Népal",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (fév. 2025)",
        "rationale": "Inscrit liste grise fév. 2025 ; défaillances stratégiques LBC/FT identifiées.",
        "fatf_date": "2025-02",
    },
    "cambodge": {
        "variations": ["cambodgien", "cambodgienne", "cambodian", "cambodge"],
        "risk_level": "LISTE_GRISE",
        "label": "Cambodge",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée | DG Trésor",
        "rationale": "Surveillance FATF pour défaillances LBC/FT ; risques BC liés aux jeux en ligne et ZES.",
        "fatf_date": "2019-06",
    },
}

def get_nationality_risk(nationality: str) -> Optional[Dict[str, Any]]:
    """
    Retourne les infos de risque pour une nationalité donnée.
    Délègue au SanctionsLoader intégré (DG Trésor + FATF, cache 24h).
    Ne bloque PAS — génère une alerte non-bloquante.
    """
    return get_sanctions_loader().get_nationality_risk(nationality)


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 1 — LISTES DE SANCTIONS DYNAMIQUES (DG Trésor + FATF)
# Chargement live au démarrage, cache TTL 24h, fallback statique intégré
# ════════════════════════════════════════════════════════════════════════════════

_SANCTIONS_TIMEOUT = 20
_SANCTIONS_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; ScreeningApp/4.0; Compliance)',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
}

# ── URL DG Trésor (lien permanent, toujours à jour) ──────────────────────────
_DGTRESOR_SANCTIONS_URL = "https://www.tresor.economie.gouv.fr/services-aux-entreprises/sanctions-economiques"

# ── FATF — base URL + calendrier des plénières (fév / juin / oct) ────────────
_FATF_BASE   = "https://www.fatf-gafi.org/en/publications/High-risk-and-other-monitored-jurisdictions"
_FATF_PLENARIES = [(2, "february"), (6, "june"), (10, "october")]

# ── Référentiel pays : clé → { label, iso2, variations } ─────────────────────
_COUNTRY_VARIATIONS: Dict[str, Dict] = {
    "russie":           {"label": "Russie",                    "iso2": "RU", "variations": ["russe", "russia", "russian", "russie"]},
    "bielorussie":      {"label": "Biélorussie",               "iso2": "BY", "variations": ["bielorusse", "belarus", "belarusian", "biélorussie", "bielorussie"]},
    "coree_nord":       {"label": "Corée du Nord (RPDC)",      "iso2": "KP", "variations": ["nord-coréen", "nord-coréenne", "north korean", "coree du nord", "corée du nord", "dprk", "rpdc", "north korea"]},
    "iran":             {"label": "Iran",                      "iso2": "IR", "variations": ["iranien", "iranienne", "iran", "iranian"]},
    "syrie":            {"label": "Syrie",                     "iso2": "SY", "variations": ["syrien", "syrienne", "syria", "syrian", "syrie"]},
    "myanmar":          {"label": "Myanmar (Birmanie)",        "iso2": "MM", "variations": ["birman", "birmane", "myanmar", "burmese", "birmanie"]},
    "venezuela":        {"label": "Venezuela",                 "iso2": "VE", "variations": ["vénézuélien", "vénézuélienne", "venezuelan", "venezuela"]},
    "zimbabwe":         {"label": "Zimbabwe",                  "iso2": "ZW", "variations": ["zimbabwéen", "zimbabwéenne", "zimbabwean", "zimbabwe"]},
    "nicaragua":        {"label": "Nicaragua",                 "iso2": "NI", "variations": ["nicaraguayen", "nicaraguayenne", "nicaraguan", "nicaragua"]},
    "soudan":           {"label": "Soudan",                    "iso2": "SD", "variations": ["soudanais", "soudanaise", "sudanese", "soudan", "sudan"]},
    "soudan_sud":       {"label": "Soudan du Sud",             "iso2": "SS", "variations": ["sud-soudanais", "south sudanese", "soudan du sud", "south sudan"]},
    "yemen":            {"label": "Yémen",                     "iso2": "YE", "variations": ["yéménite", "yemeni", "yemen", "yémen"]},
    "libye":            {"label": "Libye",                     "iso2": "LY", "variations": ["libyen", "libyenne", "libyan", "libya", "libye"]},
    "somalie":          {"label": "Somalie",                   "iso2": "SO", "variations": ["somalien", "somalienne", "somali", "somalian", "somalie"]},
    "rca":              {"label": "République centrafricaine", "iso2": "CF", "variations": ["centrafricain", "centrafricaine", "central african", "rca", "republique centrafricaine"]},
    "rdc":              {"label": "Rép. dém. du Congo",        "iso2": "CD", "variations": ["congolais", "congolaise", "congolese", "rdc", "rd congo", "republique democratique du congo"]},
    "mali":             {"label": "Mali",                      "iso2": "ML", "variations": ["malien", "malienne", "malian", "mali"]},
    "haiti":            {"label": "Haïti",                     "iso2": "HT", "variations": ["haïtien", "haïtienne", "haitian", "haiti", "haïti"]},
    "liban":            {"label": "Liban",                     "iso2": "LB", "variations": ["libanais", "libanaise", "lebanese", "lebanon", "liban"]},
    "irak":             {"label": "Irak",                      "iso2": "IQ", "variations": ["irakien", "irakienne", "iraqi", "irak", "iraq"]},
    "burundi":          {"label": "Burundi",                   "iso2": "BI", "variations": ["burundais", "burundaise", "burundian", "burundi"]},
    "guinee":           {"label": "Guinée",                    "iso2": "GN", "variations": ["guinéen", "guinéenne", "guinean", "guinee", "guinée"]},
    "guinee_bissau":    {"label": "Guinée-Bissau",             "iso2": "GW", "variations": ["bissau-guinéen", "guinea-bissau", "guinee-bissau"]},
    "niger":            {"label": "Niger",                     "iso2": "NE", "variations": ["nigérien", "nigérienne", "nigerien", "niger"]},
    "moldavie":         {"label": "Moldavie",                  "iso2": "MD", "variations": ["moldave", "moldovan", "moldova", "moldavie"]},
    "tunisie":          {"label": "Tunisie",                   "iso2": "TN", "variations": ["tunisien", "tunisienne", "tunisian", "tunisie", "tunisia"]},
    "turquie":          {"label": "Turquie",                   "iso2": "TR", "variations": ["turc", "turque", "turkish", "turquie", "turkey"]},
    "guatemala":        {"label": "Guatemala",                 "iso2": "GT", "variations": ["guatémaltèque", "guatemalan", "guatemala"]},
    "algerie":          {"label": "Algérie",                   "iso2": "DZ", "variations": ["algérien", "algérienne", "algerian", "algerie", "algérie", "algeria"]},
    "angola":           {"label": "Angola",                    "iso2": "AO", "variations": ["angolais", "angolaise", "angolan", "angola"]},
    "cote_ivoire":      {"label": "Côte d'Ivoire",             "iso2": "CI", "variations": ["ivoirien", "ivoirienne", "ivorian", "cote d'ivoire", "côte d'ivoire", "ivory coast"]},
    "kenya":            {"label": "Kenya",                     "iso2": "KE", "variations": ["kenyan", "kenyane", "kenya"]},
    "laos":             {"label": "Laos",                      "iso2": "LA", "variations": ["laotien", "laotienne", "lao", "laos"]},
    "nepal":            {"label": "Népal",                     "iso2": "NP", "variations": ["népalais", "népalaise", "nepalese", "nepal", "népal"]},
    "cameroun":         {"label": "Cameroun",                  "iso2": "CM", "variations": ["camerounais", "camerounaise", "cameroonian", "cameroun", "cameroon"]},
    "philippines":      {"label": "Philippines",               "iso2": "PH", "variations": ["philippin", "philippine", "filipino", "filipina", "philippines"]},
    "senegal":          {"label": "Sénégal",                   "iso2": "SN", "variations": ["sénégalais", "sénégalaise", "senegalese", "senegal", "sénégal"]},
    "vietnam":          {"label": "Vietnam",                   "iso2": "VN", "variations": ["vietnamien", "vietnamienne", "vietnamese", "vietnam", "viet nam"]},
    "namibie":          {"label": "Namibie",                   "iso2": "NA", "variations": ["namibien", "namibienne", "namibian", "namibie", "namibia"]},
    "bolivie":          {"label": "Bolivie",                   "iso2": "BO", "variations": ["bolivien", "bolivienne", "bolivian", "bolivie", "bolivia"]},
    "iles_vierges_brit":{"label": "Îles Vierges brit.",        "iso2": "VG", "variations": ["british virgin islands", "iles vierges britanniques", "bvi"]},
    "mozambique":       {"label": "Mozambique",                "iso2": "MZ", "variations": ["mozambicain", "mozambicaine", "mozambican", "mozambique"]},
    "bulgarie":         {"label": "Bulgarie",                  "iso2": "BG", "variations": ["bulgare", "bulgarian", "bulgarie", "bulgaria"]},
    "mongolie":         {"label": "Mongolie",                  "iso2": "MN", "variations": ["mongol", "mongole", "mongolian", "mongolie", "mongolia"]},
    "koweit":           {"label": "Koweït",                    "iso2": "KW", "variations": ["koweïtien", "koweïtienne", "kuwaiti", "koweit", "koweït", "kuwait"]},
    "papouasie":        {"label": "Papouasie-Nouvelle-Guinée", "iso2": "PG", "variations": ["papouasien", "papouasienne", "papua new guinea", "papouasie", "png"]},
}

# ── Fallback statique FATF — plénière février 2026 ────────────────────────────
_FATF_STATIC = {
    "black": ["KP", "IR", "MM"],
    "grey":  ["DZ","AO","BO","BG","CM","CI","CD","HT","KE","KW",
               "LA","LB","LY","MN","NP","NI","PG","SN","SO","SS",
               "SY","TR","VE","VN","YE","VG","NE"],
}

# ── Fallback statique DG Trésor — scrappé le 21/04/2026 ──────────────────────
_DGTRESOR_STATIC = ["BY","BI","CD","KP","GT","GW","GN","HT","IR","IQ",
                    "LB","LY","ML","MD","MM","NI","NE","CF","RU","SO",
                    "SD","SS","SY","TN","TR","VE","YE","ZW"]

# ── Table nom DG Trésor → ISO2 ────────────────────────────────────────────────
_DGTRESOR_NAME_TO_ISO2 = {
    "biélorussie":"BY","burundi":"BI","congo (république démocratique du)":"CD",
    "corée du nord":"KP","guatemala":"GT","guinée-bissau":"GW","guinée":"GN",
    "haïti":"HT","iran":"IR","irak":"IQ","liban":"LB","libye":"LY",
    "mali":"ML","moldavie":"MD","myanmar (ex-birmanie)":"MM","nicaragua":"NI",
    "niger":"NE","république centrafricaine":"CF","russie":"RU","somalie":"SO",
    "soudan":"SD","soudan du sud":"SS","syrie":"SY","tunisie":"TN",
    "turquie":"TR","venezuela":"VE","yémen":"YE","zimbabwe":"ZW",
}

# ── Table nom FATF → ISO2 ─────────────────────────────────────────────────────
_FATF_NAME_TO_ISO2 = {
    "democratic people's republic of korea":"KP","north korea":"KP","dprk":"KP",
    "iran":"IR","myanmar":"MM","burma":"MM",
    "algeria":"DZ","angola":"AO","bolivia":"BO","bulgaria":"BG",
    "cameroon":"CM","côte d'ivoire":"CI","cote d'ivoire":"CI","ivory coast":"CI",
    "democratic republic of the congo":"CD","drc":"CD",
    "haiti":"HT","kenya":"KE","kuwait":"KW",
    "laos":"LA","lao pdr":"LA","lao people's democratic republic":"LA",
    "lebanon":"LB","libya":"LY","mongolia":"MN",
    "nepal":"NP","nicaragua":"NI","niger":"NE",
    "papua new guinea":"PG","png":"PG",
    "senegal":"SN","somalia":"SO","south sudan":"SS",
    "syria":"SY","turkey":"TR",
    "venezuela":"VE","vietnam":"VN","yemen":"YE",
    "british virgin islands":"VG",
}

# ── Métadonnées par niveau de risque ─────────────────────────────────────────
_RISK_METADATA = {
    "LISTE_NOIRE":       {"action":"CONTRE_MESURES",    "source_label":"FATF – Liste noire (appel à l'action)",             "rationale":"Défaillances stratégiques graves LBC/FT/FP. Contre-mesures obligatoires."},
    "SANCTIONS_UE":      {"action":"DDR_RENFORCEE",     "source_label":"DG Trésor / Règlement UE – Sanctions géographiques","rationale":"Pays soumis à un régime de sanctions économiques et financières UE/ONU en vigueur sur le territoire français."},
    "LISTE_GRISE":       {"action":"VIGILANCE_RENFORCEE","source_label":"FATF – Liste grise (surveillance renforcée)",      "rationale":"Défaillances stratégiques LBC/FT. Travaille avec le FATF sur un plan d'action correctif."},
    "SANCTIONS_ET_GRISE":{"action":"DDR_RENFORCEE",     "source_label":"DG Trésor + FATF liste grise",                     "rationale":"Pays sous sanctions UE/ONU ET sous surveillance renforcée FATF."},
}


def _sanctions_norm(text: str) -> str:
    text = str(text).lower()
    text = unicodedata.normalize("NFKD", text)
    return "".join(c for c in text if not unicodedata.combining(c))


def _get_fatf_urls() -> Tuple[str, str, str]:
    """Détecte dynamiquement les URLs de la dernière plénière FATF publiée."""
    now = datetime.now()
    candidates = []
    for year in range(now.year, now.year - 2, -1):
        for month, month_name in reversed(_FATF_PLENARIES):
            if year == now.year and month > now.month:
                continue
            candidates.append((year, month, month_name))

    for year, month, month_name in candidates:
        black_url = f"{_FATF_BASE}/Call-for-action-{month_name}-{year}.html"
        grey_url  = f"{_FATF_BASE}/increased-monitoring-{month_name}-{year}.html"
        label     = f"{month_name.capitalize()} {year}"
        try:
            r = requests.head(black_url, timeout=8, headers=_SANCTIONS_HEADERS, allow_redirects=True)
            if r.status_code == 200:
                print(f"   ✅ FATF URLs détectées : {label}")
                return black_url, grey_url, label
        except Exception:
            pass

    print("   ⚠️  FATF URL auto-détection échouée — fallback février 2026")
    return (f"{_FATF_BASE}/Call-for-action-february-2026.html",
            f"{_FATF_BASE}/increased-monitoring-february-2026.html",
            "February 2026 (fallback)")


def _scrape_dgtresor() -> List[str]:
    try:
        resp = requests.get(_DGTRESOR_SANCTIONS_URL, timeout=_SANCTIONS_TIMEOUT, headers=_SANCTIONS_HEADERS)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        iso2_list = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            if "sanctions-economiques/" not in href:
                continue
            if any(x in href for x in ["regime-ue", "regime-onu", "lutte-contre", "russie-en-lien"]):
                if "russie" in href:
                    iso2_list.append("RU")
                continue
            text_norm = _sanctions_norm(a_tag.get_text(strip=True))
            if text_norm in _DGTRESOR_NAME_TO_ISO2:
                iso2 = _DGTRESOR_NAME_TO_ISO2[text_norm]
                if iso2 not in iso2_list:
                    iso2_list.append(iso2)
        if not iso2_list:
            print("   ⚠️  DG Trésor : aucun pays extrait — fallback statique")
            return list(_DGTRESOR_STATIC)
        print(f"   ✅ DG Trésor : {len(iso2_list)} pays sous sanctions")
        return iso2_list
    except Exception as e:
        print(f"   ⚠️  DG Trésor scraping échoué ({e}) — fallback statique")
        return list(_DGTRESOR_STATIC)


def _scrape_fatf_page(url: str, list_type: str) -> List[str]:
    try:
        resp = requests.get(url, timeout=_SANCTIONS_TIMEOUT, headers=_SANCTIONS_HEADERS)
        resp.raise_for_status()
        full_text = BeautifulSoup(resp.text, "html.parser").get_text(" ", strip=True).lower()
        iso2_list = [iso2 for name, iso2 in _FATF_NAME_TO_ISO2.items()
                     if name in full_text]
        # Dédupliquer en conservant l'ordre
        seen = set(); iso2_list = [x for x in iso2_list if not (x in seen or seen.add(x))]
        print(f"   {'✅' if iso2_list else '⚠️ '} FATF {list_type} : {len(iso2_list)} pays")
        return iso2_list
    except Exception as e:
        print(f"   ⚠️  FATF {list_type} scraping échoué ({e})")
        return []


def _build_risk_db(dgtresor: List[str], black: List[str], grey: List[str]) -> Dict:
    risk_db = {}
    for key, info in _COUNTRY_VARIATIONS.items():
        iso2 = info.get("iso2", "")
        in_black = iso2 in black
        in_dgt   = iso2 in dgtresor
        in_grey  = iso2 in grey
        if not (in_black or in_dgt or in_grey):
            continue
        if in_black:
            level = "LISTE_NOIRE"
        elif in_dgt and in_grey:
            level = "SANCTIONS_ET_GRISE"
        elif in_dgt:
            level = "SANCTIONS_UE"
        else:
            level = "LISTE_GRISE"
        meta = _RISK_METADATA[level]
        risk_db[key] = {**info, "risk_level": level, "action": meta["action"],
                        "source": meta["source_label"], "rationale": meta["rationale"],
                        "in_fatf_black": in_black, "in_fatf_grey": in_grey, "in_dgtresor": in_dgt}
    return risk_db


class SanctionsLoader:
    """Charge et met en cache les listes de pays à risque (DG Trésor + FATF). TTL 24h."""
    TTL_HOURS = 24

    def __init__(self):
        self.risk_db: Dict         = {}
        self.last_loaded: Optional[datetime] = None
        self.fatf_label: str       = ""
        self.dgtresor_count: int   = 0
        self.fatf_black_count: int = 0
        self.fatf_grey_count: int  = 0
        self.source_status: Dict   = {}

    def is_stale(self) -> bool:
        return not self.risk_db or self.last_loaded is None or \
               datetime.now() - self.last_loaded > timedelta(hours=self.TTL_HOURS)

    def load(self, force: bool = False) -> bool:
        if not force and not self.is_stale():
            return True
        print("\n" + "="*60)
        print("🔄 CHARGEMENT LISTES DE SANCTIONS (DG Trésor + FATF)")
        print("="*60)

        print("\n🇫🇷 DG Trésor — Sanctions géographiques…")
        dgtresor = _scrape_dgtresor()
        self.dgtresor_count = len(dgtresor)
        self.source_status["dgtresor"] = "✅ Live" if dgtresor != _DGTRESOR_STATIC else "⚠️ Fallback"

        print("\n🌐 FATF — Détection + scraping…")
        black_url, grey_url, self.fatf_label = _get_fatf_urls()
        black = _scrape_fatf_page(black_url, "noire") or list(_FATF_STATIC["black"])
        grey  = _scrape_fatf_page(grey_url,  "grise") or list(_FATF_STATIC["grey"])
        self.fatf_black_count = len(black)
        self.fatf_grey_count  = len(grey)
        self.source_status["fatf"] = "✅ Live" if (black != _FATF_STATIC["black"] or grey != _FATF_STATIC["grey"]) else "⚠️ Fallback"

        self.risk_db = _build_risk_db(dgtresor, black, grey)
        self.last_loaded = datetime.now()
        print(f"\n✅ {len(self.risk_db)} pays à risque chargés — FATF : {self.fatf_label}")
        print("="*60 + "\n")
        return True

    def get_nationality_risk(self, nationality: str) -> Optional[Dict]:
        if self.is_stale():
            self.load()
        if not nationality or not self.risk_db:
            return None
        nat_norm = _sanctions_norm(nationality)
        nat_tokens = set(nat_norm.split())

        for key, info in self.risk_db.items():
            for v in info["variations"]:
                v_norm = _sanctions_norm(v)
                if not v_norm:
                    continue
                v_tokens = set(v_norm.split())

                # Match exact sur la saisie complète
                if nat_norm == v_norm:
                    matched = True
                # La variation est un mot seul → doit correspondre exactement à un token de la saisie
                elif len(v_tokens) == 1:
                    matched = v_norm in nat_tokens
                # La variation est multi-mots → la saisie doit la contenir exactement
                else:
                    matched = v_norm in nat_norm

                if matched:
                    return {**{k: info[k] for k in ("label","iso2","risk_level","action","source","rationale",
                                                     "in_fatf_black","in_fatf_grey","in_dgtresor")},
                            "country_key": key,
                            "fatf_label":  self.fatf_label,
                            "loaded_at":   self.last_loaded.strftime("%d/%m/%Y à %H:%M") if self.last_loaded else "?"}
        return None

    def get_status_info(self) -> Dict:
        return {
            "last_loaded":          self.last_loaded.strftime("%d/%m/%Y à %H:%M") if self.last_loaded else "Jamais",
            "next_reload":          (self.last_loaded + timedelta(hours=self.TTL_HOURS)).strftime("%d/%m/%Y à %H:%M") if self.last_loaded else "—",
            "fatf_label":           self.fatf_label,
            "dgtresor_count":       self.dgtresor_count,
            "fatf_black_count":     self.fatf_black_count,
            "fatf_grey_count":      self.fatf_grey_count,
            "total_risk_countries": len(self.risk_db),
            "dgtresor_status":      self.source_status.get("dgtresor", "—"),
            "fatf_status":          self.source_status.get("fatf", "—"),
            "is_stale":             self.is_stale(),
        }

    def force_reload(self) -> bool:
        return self.load(force=True)


# Singleton interne — accès via get_sanctions_loader()
_sanctions_loader_instance: Optional[SanctionsLoader] = None


def get_sanctions_loader() -> SanctionsLoader:
    global _sanctions_loader_instance
    if _sanctions_loader_instance is None:
        _sanctions_loader_instance = SanctionsLoader()
        _sanctions_loader_instance.load()
    elif _sanctions_loader_instance.is_stale():
        _sanctions_loader_instance.load()
    return _sanctions_loader_instance


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 2 — MOTEUR GDA / PPE
# ════════════════════════════════════════════════════════════════════════════════


def norm(text: str) -> str:
    """Normalisation utilisée par le moteur GDA/PPE."""
    if not text:
        return ""
    text = str(text).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text


class ScreeningEngine:

    def __init__(self):
        self.entries    = []
        self.last_update = None
        self.source     = None          # "France" ou "Monaco"
        self.source_url = None          # URL effectivement utilisée

        print("\n" + "="*70)
        print("🔄 CHARGEMENT DONNÉES GDA — tentative France en premier")
        print("="*70)

        if self.load_from_france():
            print(f"\n✅ {len(self.entries)} entrées chargées (source : France)")
        else:
            print("\n⚠️  France indisponible — bascule sur Monaco…")
            if self.load_from_monaco():
                print(f"\n✅ {len(self.entries)} entrées chargées (source : Monaco)")
            else:
                print("\n❌ Échec des deux sources — aucune donnée disponible")

        print("="*70 + "\n")

    # ── Source primaire : France ─────────────────────────────────────────────

    def load_from_france(self) -> bool:
        """
        Charge le registre depuis l'API officielle DG Trésor (France).
        Endpoint : derniere-publication-flux-json
        Structure : { "Publications": { "DatePublication": "...",
                                        "PublicationDetail": [ { "Nom": ...,
                                          "Nature": ...,
                                          "RegistreDetail": [ {"TypeChamp": ..., "Valeur": [...]} ]
                                        } ] } }
        """
        try:
            print(f"🇫🇷 Tentative API France (timeout {FRANCE_TIMEOUT}s)…")

            response = requests.get(
                FRANCE_JSON_URL,
                timeout=FRANCE_TIMEOUT,
                headers={
                    'User-Agent': 'Mozilla/5.0 (compatible; ScreeningApp/4.0)',
                    'Accept': 'application/json',
                },
            )

            print(f"   Status : {response.status_code}")

            if response.status_code != 200:
                print(f"   ❌ Code HTTP inattendu ({response.status_code})")
                return False

            print(f"   Taille : {len(response.content) / 1024:.1f} KB")

            data = response.json()

            # Structure réelle de l'API DG Trésor
            pub     = data.get("Publications", {})
            items   = pub.get("PublicationDetail", [])
            date_pub = pub.get("DatePublication", "")

            count = 0
            for item in items:
                nom    = str(item.get("Nom", "")).strip()
                nature = str(item.get("Nature", ""))

                # On ne retient que les personnes physiques
                if nature != "Personne physique":
                    continue

                # ── Mapping exact du schéma JSON officiel DG Trésor ─────────
                # Champs top-level : IdRegistre | Nature | Nom | RegistreDetail
                # TypeChamp possibles (Personne physique) :
                #   PRENOM | ALIAS | SEXE | DATE_DE_NAISSANCE | LIEU_DE_NAISSANCE
                #   NATIONALITE | TITRE | ADRESSE_PP | AUTRE_IDENTITE | PASSEPORT
                #   CRYPTOMMONNAIE | MOTIFS | FONDEMENT_JURIDIQUE | REFERENCE_UE | REFERENCE_ONU

                prenoms        = []
                nationalities  = []
                dob            = ""
                alias          = []          # Alias.Alias (string)
                lieu_naissance = ""          # LieuDeNaissance.Lieu + .Pays
                titre          = ""          # Titre.Titre
                adresse        = []          # Adresse.Adresse + .Pays  (TypeChamp=ADRESSE_PP)
                passeport      = []          # Passeport.NumeroPasseport + .Commentaire
                identification = []          # AutreIdentite.NumeroCarte + .Commentaire
                fondement_jur  = []          # FondementJuridique.FondementJuridiqueLabel
                ref_ue         = []          # ReferenceUe.ReferenceUe
                ref_onu        = []          # ReferenceOnu.ReferenceOnu
                motifs         = []          # Motifs.Motifs (string)
                regime         = str(item.get("Regime", "")).strip()
                id_registre    = str(item.get("IdRegistre", "")).strip()

                for detail in item.get("RegistreDetail", []):
                    type_champ = detail.get("TypeChamp", "")
                    valeurs    = detail.get("Valeur") or []

                    # ── PRENOM → Prenom.Prenom ───────────────────────────────
                    if type_champ == "PRENOM":
                        prenoms += [v.get("Prenom", "") for v in valeurs if v.get("Prenom")]

                    # ── ALIAS → Alias.Alias (+ Commentaire optionnel) ────────
                    elif type_champ == "ALIAS":
                        for v in valeurs:
                            a = v.get("Alias", "").strip()
                            commentaire = v.get("Commentaire", "").strip()
                            if a:
                                alias.append(f"{a} ({commentaire})" if commentaire else a)

                    # ── DATE_DE_NAISSANCE → Annee/Mois/Jour (+ Commentaire) ──
                    elif type_champ == "DATE_DE_NAISSANCE":
                        v = valeurs[0] if valeurs else {}
                        y = v.get("Annee", "")
                        m = v.get("Mois", "")
                        j = v.get("Jour", "")
                        commentaire = v.get("Commentaire", "")
                        if y:
                            if m and j:
                                dob = f"{y}-{str(m).zfill(2)}-{str(j).zfill(2)}"
                            elif m:
                                dob = f"{y}-{str(m).zfill(2)}"
                            else:
                                dob = str(y)
                            if commentaire:
                                dob += f" ({commentaire})"

                    # ── LIEU_DE_NAISSANCE → Lieu + Pays ─────────────────────
                    elif type_champ == "LIEU_DE_NAISSANCE":
                        v = valeurs[0] if valeurs else {}
                        lieu = v.get("Lieu", "").strip()   # clé exacte du schéma : "Lieu"
                        pays = v.get("Pays", "").strip()
                        if lieu and pays:
                            lieu_naissance = f"{lieu} ({pays})"
                        elif lieu:
                            lieu_naissance = lieu
                        elif pays:
                            lieu_naissance = pays

                    # ── NATIONALITE → Pays (+ Commentaire) ──────────────────
                    elif type_champ == "NATIONALITE":
                        for v in valeurs:
                            pays = v.get("Pays", "").strip()
                            commentaire = v.get("Commentaire", "").strip()
                            if pays:
                                nationalities.append(
                                    f"{pays} ({commentaire})" if commentaire else pays
                                )

                    # ── TITRE → Titre.Titre ──────────────────────────────────
                    elif type_champ == "TITRE":
                        titres = [v.get("Titre", "") for v in valeurs if v.get("Titre")]
                        if titres:
                            titre = " | ".join(titres)

                    # ── ADRESSE_PP → Adresse + Pays ──────────────────────────
                    elif type_champ == "ADRESSE_PP":
                        for v in valeurs:
                            parts = [v.get("Adresse", ""), v.get("Pays", "")]
                            addr = ", ".join(p for p in parts if p)
                            if addr:
                                adresse.append(addr)

                    # ── PASSEPORT → NumeroPasseport (+ Commentaire) ──────────
                    elif type_champ == "PASSEPORT":
                        for v in valeurs:
                            num = v.get("NumeroPasseport", "").strip()
                            commentaire = v.get("Commentaire", "").strip()
                            if num:
                                passeport.append(
                                    f"{num} ({commentaire})" if commentaire else num
                                )

                    # ── AUTRE_IDENTITE → NumeroCarte (+ Commentaire) ─────────
                    elif type_champ == "AUTRE_IDENTITE":
                        for v in valeurs:
                            num = v.get("NumeroCarte", "").strip()
                            commentaire = v.get("Commentaire", "").strip()
                            if num:
                                identification.append(
                                    f"{num} ({commentaire})" if commentaire else num
                                )

                    # ── MOTIFS → Motifs.Motifs ───────────────────────────────
                    elif type_champ == "MOTIFS":
                        for v in valeurs:
                            m_txt = v.get("Motifs", "").strip()   # clé exacte : "Motifs"
                            if m_txt:
                                motifs.append(m_txt)

                    # ── FONDEMENT_JURIDIQUE → FondementJuridiqueLabel ────────
                    elif type_champ == "FONDEMENT_JURIDIQUE":
                        for v in valeurs:
                            label = v.get("FondementJuridiqueLabel", "").strip()
                            if label:
                                fondement_jur.append(label)

                    # ── REFERENCE_UE → ReferenceUe.ReferenceUe ──────────────
                    elif type_champ == "REFERENCE_UE":
                        for v in valeurs:
                            ref = v.get("ReferenceUe", "").strip()
                            if ref:
                                ref_ue.append(ref)

                    # ── REFERENCE_ONU → ReferenceOnu.ReferenceOnu ───────────
                    elif type_champ == "REFERENCE_ONU":
                        for v in valeurs:
                            ref = v.get("ReferenceOnu", "").strip()
                            if ref:
                                ref_onu.append(ref)

                if not nom and not prenoms:
                    continue

                # Consolider fondement juridique + références UE/ONU
                fondement_complet = " | ".join(filter(None, fondement_jur + ref_ue + ref_onu))

                # Une entrée par prénom
                for prenom in (prenoms if prenoms else [""]):
                    premier = prenom.split()[0] if prenom else ""
                    self.entries.append({
                        # ── Identité ────────────────────────────────────────
                        'id_registre':     id_registre,
                        'regime':          regime,
                        'nom':             nom,
                        'prenom':          premier,
                        'prenom_complet':  prenom,
                        'alias':           [a for a in alias if a],
                        'titre':           titre,
                        # ── Naissance ───────────────────────────────────────
                        'date_naissance':  dob,
                        'lieu_naissance':  lieu_naissance,
                        # ── Nationalité ─────────────────────────────────────
                        'nationalite':     [n for n in nationalities if n],
                        # ── Documents ───────────────────────────────────────
                        'passeport':       passeport,
                        'identification':  identification,
                        # ── Localisation ────────────────────────────────────
                        'adresse':         adresse,
                        # ── Sanctions ───────────────────────────────────────
                        'motifs':          " ".join(motifs),         # Motifs.Motifs
                        'fondement_jur':   fondement_complet,        # FondementJuridiqueLabel + ReferenceUe + ReferenceOnu
                    })
                    count += 1

            if count == 0:
                print("   ⚠️  Aucune personne physique extraite du JSON France")
                return False

            print(f"   ✅ {count} personnes physiques (France)")

            # Date de dernière publication
            if date_pub:
                self.last_update = str(date_pub)[:16].replace("T", " à ").replace("-", "/")
            else:
                try:
                    r = requests.get(FRANCE_DATE_URL, timeout=10)
                    if r.ok:
                        self.last_update = (r.json().get("datePublication") or r.text.strip())[:16]
                except Exception:
                    pass

            self.source     = "France"
            self.source_url = FRANCE_JSON_URL
            return True

        except requests.exceptions.Timeout:
            print(f"   ⏱️  Timeout après {FRANCE_TIMEOUT}s — API France non disponible")
            return False
        except requests.exceptions.ConnectionError as e:
            print(f"   🔌 Connexion impossible à l'API France : {e}")
            return False
        except Exception as e:
            print(f"   ❌ Erreur inattendue (France) : {e}")
            return False

    # ── Source de repli : Monaco ─────────────────────────────────────────────

    def load_from_monaco(self) -> bool:
        """
        Charge le registre depuis l'API officielle de Monaco.
        Utilisé uniquement si l'API France a échoué.
        """
        try:
            print(f"🇲🇨 Tentative API Monaco (timeout {MONACO_TIMEOUT}s)…")

            response = requests.get(
                MONACO_JSON_URL,
                timeout=MONACO_TIMEOUT,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json',
                    'Referer': 'https://geldefonds.gouv.mc/',
                },
            )

            print(f"   Status : {response.status_code}")

            if response.status_code != 200:
                print(f"   ❌ Code HTTP inattendu ({response.status_code})")
                return False

            print(f"   Taille : {len(response.content) / 1024:.1f} KB")

            data = response.json()
            measures = data if isinstance(data, list) else []

            count = 0
            for m in measures:
                if m.get('nature') == 'Personne physique':
                    nom     = str(m.get('nom', '')).strip()
                    details = m.get('mesureDetails', {})
                    prenom  = str(details.get('prenom', '')).strip()
                    date    = str(details.get('dateNaissance', '')).strip()
                    nat     = str(details.get('nationalite', '')).strip()
                    alias_raw = details.get('alias', [])
                    alias   = [str(a) for a in alias_raw] if isinstance(alias_raw, list) else []
                    # Colonnes registre : Motifs | Fondement juridique | Régime | Titre | Adresse | Passeport
                    motifs        = str(m.get('motif', '') or m.get('motifs', '') or m.get('commentaire', '')).strip()
                    fondement_jur = str(m.get('reference', '') or m.get('reglementRef', '') or m.get('fondementJuridique', '')).strip()
                    regime        = str(m.get('regime', '')).strip()
                    titre         = str(details.get('titre', '')).strip()
                    lieu_naiss    = str(details.get('lieuNaissance', '')).strip()
                    adresse_raw   = details.get('adresse', '')
                    adresse       = [str(adresse_raw)] if adresse_raw else []
                    passeport_raw = details.get('passeport', '')
                    passeport     = [str(passeport_raw)] if passeport_raw else []

                    if nom or prenom:
                        premier = prenom.split()[0] if prenom else ""
                        self.entries.append({
                            'id_registre':    str(m.get('id', '')),
                            'regime':         regime,
                            'nom':            nom,
                            'prenom':         premier,
                            'prenom_complet': prenom,
                            'alias':          [a for a in alias if a],
                            'titre':          titre,
                            'date_naissance': date,
                            'lieu_naissance': lieu_naiss,
                            'nationalite':    [nat] if nat else [],
                            'passeport':      passeport,
                            'identification': [],
                            'adresse':        adresse,
                            'fondement_jur':  fondement_jur,
                            'motifs':         motifs,
                        })
                        count += 1

            if count == 0:
                print("   ⚠️  Aucune personne physique extraite du JSON Monaco")
                return False

            print(f"   ✅ {count} personnes physiques (Monaco)")

            # Date de dernière publication
            try:
                r = requests.get(MONACO_DATE_URL, timeout=10)
                if r.status_code == 200:
                    txt = r.text.strip().strip('"')
                    if '-' in txt:
                        parts = txt.split(' ')
                        d = parts[0].replace('-', '/')
                        t = parts[1][:5] if len(parts) > 1 else ''
                        self.last_update = f"{d} à {t}"
            except Exception:
                pass

            self.source     = "Monaco"
            self.source_url = MONACO_JSON_URL
            return True

        except requests.exceptions.Timeout:
            print(f"   ⏱️  Timeout après {MONACO_TIMEOUT}s — API Monaco non disponible")
            return False
        except requests.exceptions.ConnectionError as e:
            print(f"   🔌 Connexion impossible à l'API Monaco : {e}")
            return False
        except Exception as e:
            print(f"   ❌ Erreur inattendue (Monaco) : {e}")
            return False

    def get_last_update_info(self) -> str:
        flag = "🇫🇷" if self.source == "France" else "🇲🇨"
        src  = self.source or "Inconnue"
        return f"{self.last_update} ({flag} {src})" if self.last_update else f"{flag} {src}"

    def is_ready(self) -> bool:
        return len(self.entries) > 0

    def detect_ppe_risk(self, client_data: Dict[str, Any]) -> Tuple[bool, int, List[str], str]:
        profession = norm(client_data.get("profession", ""))

        if not profession:
            return False, 0, [], ""

        risk_score = 0
        matched = []

        # ÉTAPE 1: Vérifier les mots PPE clairs AVANT les ambigus
        for cat, kws in PPE_KEYWORDS.items():
            for kw in kws:
                kw_norm = norm(kw)
                if kw_norm in profession:
                    # Calculer le score selon la catégorie (aligné avec PPE_KEYWORDS)
                    if cat in ["président", "ministre", "premier_ministre"]:
                        score_boost = 25  # Niveau 1
                    elif cat in ["député", "sénateur", "ambassadeur", "général", "juge", "procureur"]:
                        score_boost = 20  # Niveau 2
                    elif cat in ["maire", "préfet", "gouverneur", "consul", "colonel", "commissaire",
                                 "directeur_agence", "président_conseil"]:
                        score_boost = 15  # Niveau 3
                    else:
                        score_boost = 10  # Niveau 4

                    risk_score += score_boost
                    matched.append(kw)

        # ÉTAPE 2: Mots ambigus (seulement si aucun PPE clair détecté)
        if risk_score == 0:
            mots_ambigus = ["directeur", "director", "president", "chairman", "dg", "ceo"]

            for mot_ambigu in mots_ambigus:
                if norm(mot_ambigu) in profession:
                    # Vérifier contexte
                    contexte_prive = ["entreprise", "societe", "société", "company", "business", "startup", "privé", "private"]
                    contexte_public = ["ministere", "ministère", "ministry", "etat", "état", "gouvernement",
                                       "government", "public", "administration", "mairie", "region", "région"]

                    has_private = any(norm(c) in profession for c in contexte_prive)
                    has_public = any(norm(c) in profession for c in contexte_public)

                    # Si pas de contexte → clarification
                    if not has_private and not has_public:
                        return False, 0, [], "CLARIFICATION_REQUISE"
                    elif has_public and not has_private:
                        # Public → PPE
                        risk_score += 20
                        matched.append(mot_ambigu)

        return risk_score >= 15, risk_score, matched, ""

    def check_gda_live(self, first_name: str, last_name: str) -> Tuple[bool, int, str]:
        """
        DÉTECTION EN TEMPS RÉEL pendant la saisie
        Supporte prénoms composés (ex: "Vyacheslav Nikolaevich")
        """
        if not first_name and not last_name:
            return False, 0, ""

        if not self.entries:
            return False, 0, ""

        first_norm = norm(first_name) if first_name else ""
        last_norm = norm(last_name) if last_name else ""

        # Chercher dans les entrées
        best_score = 0
        best_name = ""

        for entry in self.entries:
            # IMPORTANT: Vérifier à la fois le premier prénom ET le prénom complet
            entry_first = norm(entry.get('prenom', ''))  # Premier prénom
            entry_first_complet = norm(entry.get('prenom_complet', ''))  # Tous les prénoms
            entry_last = norm(entry.get('nom', ''))

            score = 0

            # Match prénom - vérifier contre TOUS les prénoms
            if first_norm:
                # Essayer avec premier prénom
                if entry_first and first_norm == entry_first:
                    score += 50
                elif entry_first and (first_norm in entry_first or entry_first in first_norm):
                    score += 40
                elif entry_first and len(first_norm) >= 3 and len(entry_first) >= 3 and first_norm[:3] == entry_first[:3]:
                    score += 30

                # Essayer aussi avec prénom complet (pour prénoms composés)
                if entry_first_complet:
                    # Si le prénom recherché est dans les prénoms composés
                    if first_norm in entry_first_complet:
                        score = max(score, 45)  # Bon match
                    # Vérifier chaque partie du prénom composé
                    for part in entry_first_complet.split():
                        if first_norm == part:
                            score = max(score, 50)  # Match exact sur une partie
                        elif first_norm in part or part in first_norm:
                            score = max(score, 40)

            # Match nom
            if last_norm and entry_last:
                if last_norm == entry_last:
                    score += 50
                elif last_norm in entry_last or entry_last in last_norm:
                    score += 40
                elif len(last_norm) >= 3 and len(entry_last) >= 3 and last_norm[:3] == entry_last[:3]:
                    score += 30

            if score > best_score:
                best_score = score
                # Afficher le prénom complet dans le message
                prenom_display = entry.get('prenom_complet', entry.get('prenom', ''))
                best_name = f"{prenom_display} {entry.get('nom', '')}".strip()

        # Retourner si score >= 60 (détection précoce)
        if best_score >= 60:
            return True, best_score, best_name

        return False, best_score, best_name

    def screen_client(self, client_data: Dict[str, Any],
                      is_public_sector: Optional[bool] = None,
                      ppe_answers: Optional[Dict[str, bool]] = None) -> Dict[str, Any]:

        first_name = client_data.get("first_name", "")
        last_name = client_data.get("last_name", "")

        # PPE
        is_ppe, ppe_score, ppe_kws, ppe_signal = self.detect_ppe_risk(client_data)

        # Si clarification requise et pas de réponse fournie
        if ppe_signal == "CLARIFICATION_REQUISE" and is_public_sector is None:
            return {
                "decision": "PENDING_CLARIFICATION",
                "score": 0,
                "gda_score": 0,
                "decision_reason": "Profession ambiguë : précisez si secteur public ou privé",
                "alert_type": "PPE_CLARIFICATION",
                "can_subscribe": False
            }

        # Si clarification fournie et secteur public confirmé
        elif ppe_signal == "CLARIFICATION_REQUISE" and is_public_sector is True:
            is_ppe = True
            ppe_score = 20
            ppe_kws = ["Directeur secteur public"]

        if ppe_signal == "CLARIFICATION_REQUISE" and is_public_sector is False:
            is_ppe = False
            ppe_score = 0
            ppe_kws = []

        # GDA avec RapidFuzz
        if not self.entries:
            return {"decision": "OK", "can_subscribe": True}

        entry_names_norm = []
        for entry in self.entries:
            name = f"{entry.get('prenom', '')} {entry.get('nom', '')}".strip()
            name_norm = norm(name)
            if name_norm:
                entry_names_norm.append((name_norm, entry))

        client_name = f"{first_name} {last_name}".strip()
        client_norm = norm(client_name)

        names_only = [n for n, _ in entry_names_norm]
        matches = process.extract(
            client_norm,
            names_only,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=30,
            limit=5
        )

        best_score = 0
        best_match = None
        matched_fields = []

        for matched_norm, score, _ in matches:
            for name_norm, entry in entry_names_norm:
                if name_norm == matched_norm:
                    total_score = score

                    # Bonus date
                    if client_data.get("birth_date") and entry.get("date_naissance"):
                        client_dob = client_data["birth_date"].replace("-", "/")
                        entry_dob = entry["date_naissance"].replace("-", "/")

                        if client_dob == entry_dob:
                            total_score += 20
                            matched_fields.append("birth_date")
                        elif len(client_dob) >= 4 and len(entry_dob) >= 4 and client_dob[-4:] == entry_dob[-4:]:
                            total_score += 5
                            matched_fields.append("birth_year")

                    # Bonus nationalité
                    if client_data.get("nationality") and entry.get("nationalite"):
                        client_nat = norm(client_data["nationality"])
                        entry_nats = [norm(n) for n in entry["nationalite"] if n]

                        if any(client_nat[:3] in n or n[:3] in client_nat
                               for n in entry_nats if len(n) >= 3 and len(client_nat) >= 3):
                            total_score += 10
                            matched_fields.append("nationality")

                    if total_score > best_score:
                        best_score = total_score
                        best_match = entry
                    break

        # Décision
        decision = "OK"
        reason = "Aucune correspondance"
        alert = "NONE"
        match_name = ""

        # Construire les détails enrichis du match GDA — colonnes exactes du registre
        gda_details = None
        if best_match and best_score >= SEUIL_REVIEW:
            prenom_display = best_match.get('prenom_complet', best_match.get('prenom', ''))
            match_name = f"{prenom_display} {best_match.get('nom', '')}".strip()

            gda_details = {
                # ── Identité ────────────────────────────────────────────────
                "id_registre":    best_match.get('id_registre', ''),
                "regime":         best_match.get('regime', ''),
                "nom_complet":    match_name,
                "alias":          best_match.get('alias', []),
                "titre":          best_match.get('titre', ''),
                # ── Naissance ───────────────────────────────────────────────
                "date_naissance": best_match.get('date_naissance', ''),
                "lieu_naissance": best_match.get('lieu_naissance', ''),
                # ── Nationalité ─────────────────────────────────────────────
                "nationalite":    best_match.get('nationalite', []),
                # ── Documents d'identité ────────────────────────────────────
                "passeport":      best_match.get('passeport', []),
                "identification": best_match.get('identification', []),
                # ── Localisation ────────────────────────────────────────────
                "adresse":        best_match.get('adresse', []),
                # ── Sanctions ───────────────────────────────────────────────
                "fondement_jur":  best_match.get('fondement_jur', ''),   # Fondement juridique
                "motifs":         best_match.get('motifs', ''),           # Motifs
                # ── Méta ────────────────────────────────────────────────────
                "source":         self.source or "Inconnue",
                "champs_correspondants": matched_fields,
            }

        if best_score >= SEUIL_BLOCK:
            decision = "BLOCK"
            reason = f"Match GDA fort — Score de similarité : {best_score}/100"
            alert = "GDA"
        elif best_score >= SEUIL_REVIEW:
            decision = "REVIEW"
            reason = f"Match GDA possible — Score de similarité : {best_score}/100"
            alert = "GDA"

        # PPE
        if is_ppe:
            if alert == "GDA":
                alert = "BOTH"
                reason += f" + PPE détecté (score PPE : {ppe_score})"
            else:
                alert = "PPE"
                decision = "REVIEW"
                reason = f"PPE détecté — Score risque : {ppe_score} — Fonctions : {', '.join(ppe_kws)}"

        if ppe_answers and any(ppe_answers.values()):
            if decision == "OK":
                decision = "REVIEW"
                alert = "PPE"

        # Nationalité : NE BLOQUE PAS — alerte informative uniquement
        nationality_risk = get_nationality_risk(client_data.get("nationality", ""))

        return {
            "decision": decision,
            "score": best_score,
            "gda_score": best_score,
            "gda_match": best_score >= SEUIL_REVIEW,
            "gda_details": gda_details,
            "decision_reason": reason,
            "alert_type": alert,
            "match_name": match_name,
            "matched_fields": matched_fields,
            "ppe_keywords": ppe_kws,
            "ppe_score": ppe_score,
            "nationality_risk": nationality_risk,   # Alerte non-bloquante
            "can_subscribe": decision == "OK"
        }


if __name__ == "__main__":
    engine = ScreeningEngine()

    if engine.is_ready():
        print(f"✅ Prêt - {len(engine.entries)} entrées")#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Moteur de screening GDA/PPE + Chargement dynamique des sanctions (DG Trésor / FATF)

Sources GDA :
  - API France (DG Trésor) — source primaire
  - API Monaco               — source de repli automatique

Sources sanctions pays :
  - DG Trésor sanctions géographiques (lien permanent, scraping HTML)
  - FATF liste noire + liste grise (URL auto-détectée selon calendrier des plénières)
  Cache TTL 24h avec fallback statique.
"""

import requests
import unicodedata
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
from rapidfuzz import fuzz, process
from bs4 import BeautifulSoup

# ── API FRANCE (source primaire) ────────────────────────────────────────────
# Endpoint correct : "flux-json" (et non "fichier-json")
FRANCE_JSON_URL  = "https://gels-avoirs.dgtresor.gouv.fr/ApiPublic/api/v1/publication/derniere-publication-flux-json"
FRANCE_DATE_URL  = "https://gels-avoirs.dgtresor.gouv.fr/ApiPublic/api/v1/publication/derniere-publication-date"

# ── API MONACO (source de repli) ────────────────────────────────────────────
MONACO_JSON_URL  = "https://geldefonds.gouv.mc/directdownload/sanctions.json"
MONACO_DATE_URL  = "https://geldefonds.gouv.mc/api/derniere-publication-date"

# ── Timeouts ─────────────────────────────────────────────────────────────────
FRANCE_TIMEOUT   = 30   # secondes avant de basculer sur Monaco
MONACO_TIMEOUT   = 30   # timeout Monaco (source de secours, plus tolérant)

# ── Seuils de décision ───────────────────────────────────────────────────────
SEUIL_BLOCK  = 85
SEUIL_REVIEW = 75

# PPE Keywords - Liste complète
PPE_KEYWORDS = {
    # Niveau 1 - Très haut risque (score 25)
    # "président": ["président", "president", "présidente"],
    "ministre": ["ministre", "minister", "secretary"],
    "premier_ministre": ["premier ministre", "prime minister", "chef du gouvernement"],

    # Niveau 2 - Haut risque (score 20)
    "député": ["député", "députée", "depute", "deputy", "mp", "membre du parlement"],
    "sénateur": ["sénateur", "sénatrice", "senator"],
    "ambassadeur": ["ambassadeur", "ambassadrice", "ambassador"],
    "général": ["général", "générale", "general"],
    "juge": ["juge", "judge", "magistrat", "magistrate"],
    "procureur": ["procureur", "procureure", "prosecutor", "procurator"],

    # Niveau 3 - Risque modéré (score 15)
    "maire": ["maire", "mayor", "mairesse"],
    "préfet": ["préfet", "préfète", "prefect"],
    "gouverneur": ["gouverneur", "gouverneure", "governor"],
    "consul": ["consul", "consule"],
    "colonel": ["colonel", "colonelle"],
    "commissaire": ["commissaire", "commissioner"],
    "directeur_agence": ["directeur d'agence", "director of agency", "chef d'agence"],
    "président_conseil": ["président du conseil", "council president", "président conseil régional"],

    # Niveau 4 - Risque surveillé (score 10)
    "conseiller": ["conseiller", "conseillère", "councillor", "councilor", "advisor"],
    "adjoint_maire": ["adjoint au maire", "deputy mayor", "adjoint"],
    "chef_cabinet": ["chef de cabinet", "chief of staff"],
    "secrétaire_état": ["secrétaire d'état", "secretary of state"],
    "sous_préfet": ["sous-préfet", "sous-préfète", "sub-prefect"],
}

# ── Pays à risque - Structure détaillée ─────────────────────────────────────
# Chaque pays : variations de nationalité + métadonnées de risque
# risk_level : "LISTE_NOIRE" | "SANCTIONS_UE" | "LISTE_GRISE"
# action : "CONTRE_MESURES" | "VIGILANCE_RENFORCEE" | "DDR_RENFORCEE"
# source : référence réglementaire applicable
# rationale : motif synthétique affiché dans l'alerte

SENSITIVE_NATIONALITIES = {
    # ── LISTE NOIRE FATF (appel à contre-mesures) ───────────────────────────
    "coree_nord": {
        "variations": ["nord-coréen", "nord-coréenne", "north korean", "coree du nord", "corée du nord", "dprk", "rpdc"],
        "risk_level": "LISTE_NOIRE",
        "label": "Corée du Nord (RPDC)",
        "action": "CONTRE_MESURES",
        "source": "FATF – Appel à l'action (oct. 2025) | Résol. CSNU 2270",
        "rationale": "Défaillances stratégiques graves LBC/FT/FP ; risques élevés de financement de la prolifération nucléaire.",
        "fatf_date": "2025-10",
    },
    "iran": {
        "variations": ["iranien", "iranienne", "iran", "iranian"],
        "risk_level": "LISTE_NOIRE",
        "label": "Iran",
        "action": "CONTRE_MESURES",
        "source": "FATF – Appel à l'action (oct. 2025) | DG Trésor – Sanctions nucléaires",
        "rationale": "Plan d'action non résolu depuis 2016 ; risques de financement de prolifération. Obligations découlant des résolutions CSNU sur le nucléaire iranien.",
        "fatf_date": "2025-10",
    },
    "myanmar": {
        "variations": ["birman", "birmane", "myanmar", "burmese", "birmanie"],
        "risk_level": "LISTE_NOIRE",
        "label": "Myanmar (Birmanie)",
        "action": "CONTRE_MESURES",
        "source": "FATF – Appel à l'action (oct. 2025) | DG Trésor – Sanctions",
        "rationale": "Absence persistante de progrès sur le plan d'action FATF ; vigilance renforcée obligatoire sur toutes les transactions.",
        "fatf_date": "2025-10",
    },

    # ── SANCTIONS UE / DG TRÉSOR (embargo, gel d'avoirs) ───────────────────
    "russie": {
        "variations": ["russe", "russia", "russian", "russie"],
        "risk_level": "SANCTIONS_UE",
        "label": "Russie",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°833/2014 et suivants | DG Trésor – Sanctions Russie",
        "rationale": "Embargo et sanctions économiques UE en lien avec l'agression contre l'Ukraine. Gel d'avoirs de nombreuses personnes physiques et entités.",
        "fatf_date": "2022-03",
    },
    "bielorussie": {
        "variations": ["bielorusse", "belarus", "belarusian", "biélorussie", "bielorussie"],
        "risk_level": "SANCTIONS_UE",
        "label": "Biélorussie",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°765/2006 et suivants | DG Trésor – Sanctions Biélorussie",
        "rationale": "Sanctions UE liées aux violations des droits de l'Homme et au soutien à l'agression russe contre l'Ukraine.",
        "fatf_date": "2020-06",
    },
    "syrie": {
        "variations": ["syrien", "syrienne", "syria", "syrian", "syrie"],
        "risk_level": "SANCTIONS_UE",
        "label": "Syrie",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°36/2012 | DG Trésor – Sanctions Syrie | FATF liste grise",
        "rationale": "Embargo et sanctions UE. Pays également sous surveillance renforcée FATF pour défaillances LBC/FT.",
        "fatf_date": "2024-06",
    },
    "libye": {
        "variations": ["libyen", "libyenne", "libyan", "libya", "libye"],
        "risk_level": "SANCTIONS_UE",
        "label": "Libye",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°204/2011 | DG Trésor – Sanctions Libye",
        "rationale": "Embargo sur les armes et sanctions UE. Instabilité politique et risques élevés de blanchiment.",
        "fatf_date": "2011-03",
    },
    "soudan": {
        "variations": ["soudanais", "soudanaise", "sudanese", "sudan", "soudan"],
        "risk_level": "SANCTIONS_UE",
        "label": "Soudan",
        "action": "DDR_RENFORCEE",
        "source": "DG Trésor – Sanctions Soudan | Résol. CSNU 1556",
        "rationale": "Sanctions internationales ; risques élevés BC/FT liés à l'instabilité et aux conflits armés.",
        "fatf_date": "2004-07",
    },
    "zimbabwe": {
        "variations": ["zimbabwéen", "zimbabwéenne", "zimbabwean", "zimbabwe"],
        "risk_level": "SANCTIONS_UE",
        "label": "Zimbabwe",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°314/2004 | DG Trésor – Sanctions Zimbabwe",
        "rationale": "Sanctions UE liées à des violations des droits de l'Homme. Gel d'avoirs de personnes désignées.",
        "fatf_date": "2004-02",
    },
    "nicaragua": {
        "variations": ["nicaraguayen", "nicaraguayenne", "nicaraguan", "nicaragua"],
        "risk_level": "SANCTIONS_UE",
        "label": "Nicaragua",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°2022/1304 | DG Trésor – Sanctions Nicaragua",
        "rationale": "Sanctions UE liées aux violations des droits de l'Homme et à la répression politique.",
        "fatf_date": "2022-07",
    },
    "somalie": {
        "variations": ["somalien", "somalienne", "somali", "somalian", "somalie"],
        "risk_level": "SANCTIONS_UE",
        "label": "Somalie",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°356/2010 | DG Trésor – Sanctions Somalie | Résol. CSNU 1844",
        "rationale": "Embargo et sanctions liés à la piraterie et à Al-Shabaab. Risques élevés de financement du terrorisme.",
        "fatf_date": "2010-04",
    },
    "rca": {
        "variations": ["centrafricain", "centrafricaine", "central african", "république centrafricaine", "rca"],
        "risk_level": "SANCTIONS_UE",
        "label": "République centrafricaine",
        "action": "DDR_RENFORCEE",
        "source": "Règlement UE n°224/2014 | DG Trésor – Sanctions RCA | Résol. CSNU 2127",
        "rationale": "Embargo sur les armes et sanctions liés au conflit armé. Risques élevés BC/FT.",
        "fatf_date": "2013-12",
    },

    # ── LISTE GRISE FATF (surveillance renforcée, fév. 2025) ────────────────
    "congo_rdc": {
        "variations": ["congolais", "congolaise", "congolese", "république démocratique du congo", "rdc", "rd congo"],
        "risk_level": "LISTE_GRISE",
        "label": "République démocratique du Congo",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (fév. 2025)",
        "rationale": "Défaillances stratégiques LBC/FT identifiées par le FATF. Diligence renforcée obligatoire.",
        "fatf_date": "2022-06",
    },
    "mali": {
        "variations": ["malien", "malienne", "malian", "mali"],
        "risk_level": "LISTE_GRISE",
        "label": "Mali",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (fév. 2025)",
        "rationale": "Plan d'action FATF expiré avec travail restant. Défaillances sur le financement du terrorisme.",
        "fatf_date": "2021-06",
    },
    "haiti": {
        "variations": ["haïtien", "haïtienne", "haitian", "haiti", "haïti"],
        "risk_level": "LISTE_GRISE",
        "label": "Haïti",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (fév. 2025)",
        "rationale": "Défaillances stratégiques LBC/FT. Instabilité politique aggravant les risques.",
        "fatf_date": "2020-10",
    },
    "liban": {
        "variations": ["libanais", "libanaise", "lebanese", "lebanon", "liban"],
        "risk_level": "LISTE_GRISE",
        "label": "Liban",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (oct. 2024)",
        "rationale": "Inscrit sur liste grise FATF en oct. 2024 suite à défaillances LBC/FT identifiées.",
        "fatf_date": "2024-10",
    },
    "venezuela": {
        "variations": ["vénézuélien", "vénézuélienne", "venezuelan", "venezuela"],
        "risk_level": "LISTE_GRISE",
        "label": "Venezuela",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (juin 2024)",
        "rationale": "Inscrit liste grise FATF juin 2024 : risques BC liés à l'économie informelle et liens Iran/Caracas.",
        "fatf_date": "2024-06",
    },
    "yemen": {
        "variations": ["yéménite", "yemeni", "yemen", "yémen"],
        "risk_level": "LISTE_GRISE",
        "label": "Yémen",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée | DG Trésor – Sanctions",
        "rationale": "Sous surveillance FATF ; conflit armé et risques élevés de financement du terrorisme.",
        "fatf_date": "2020-10",
    },
    "kenya": {
        "variations": ["kenyan", "kenyane", "kenyan", "kenya"],
        "risk_level": "LISTE_GRISE",
        "label": "Kenya",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (fév. 2024)",
        "rationale": "Inscrit liste grise FATF fév. 2024 ; défaillances sur la supervision des risques BC/FT.",
        "fatf_date": "2024-02",
    },
    "algerie": {
        "variations": ["algérien", "algérienne", "algerian", "algerie", "algérie"],
        "risk_level": "LISTE_GRISE",
        "label": "Algérie",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (oct. 2024)",
        "rationale": "Inscrit liste grise FATF oct. 2024 ; défaillances stratégiques LBC/FT identifiées.",
        "fatf_date": "2024-10",
    },
    "angola": {
        "variations": ["angolais", "angolaise", "angolan", "angola"],
        "risk_level": "LISTE_GRISE",
        "label": "Angola",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (oct. 2024)",
        "rationale": "Inscrit liste grise FATF oct. 2024 ; lacunes en matière de supervision et poursuites BC.",
        "fatf_date": "2024-10",
    },
    "cote_ivoire": {
        "variations": ["ivoirien", "ivoirienne", "ivorian", "cote d'ivoire", "côte d'ivoire"],
        "risk_level": "LISTE_GRISE",
        "label": "Côte d'Ivoire",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (oct. 2024)",
        "rationale": "Inscrit liste grise FATF oct. 2024 malgré progrès notables post-REM 2023.",
        "fatf_date": "2024-10",
    },
    "laos": {
        "variations": ["laotien", "laotienne", "lao", "laos"],
        "risk_level": "LISTE_GRISE",
        "label": "Laos (RDP Lao)",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (fév. 2025)",
        "rationale": "Inscrit liste grise fév. 2025 ; défis persistants sur l'évaluation des risques et la supervision réglementaire.",
        "fatf_date": "2025-02",
    },
    "nepal": {
        "variations": ["népalais", "népalaise", "nepalese", "nepal", "népal"],
        "risk_level": "LISTE_GRISE",
        "label": "Népal",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée (fév. 2025)",
        "rationale": "Inscrit liste grise fév. 2025 ; défaillances stratégiques LBC/FT identifiées.",
        "fatf_date": "2025-02",
    },
    "cambodge": {
        "variations": ["cambodgien", "cambodgienne", "cambodian", "cambodge"],
        "risk_level": "LISTE_GRISE",
        "label": "Cambodge",
        "action": "VIGILANCE_RENFORCEE",
        "source": "FATF – Surveillance renforcée | DG Trésor",
        "rationale": "Surveillance FATF pour défaillances LBC/FT ; risques BC liés aux jeux en ligne et ZES.",
        "fatf_date": "2019-06",
    },
}

def get_nationality_risk(nationality: str) -> Optional[Dict[str, Any]]:
    """
    Retourne les infos de risque pour une nationalité donnée.
    Délègue au SanctionsLoader intégré (DG Trésor + FATF, cache 24h).
    Ne bloque PAS — génère une alerte non-bloquante.
    """
    return get_sanctions_loader().get_nationality_risk(nationality)


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 1 — LISTES DE SANCTIONS DYNAMIQUES (DG Trésor + FATF)
# Chargement live au démarrage, cache TTL 24h, fallback statique intégré
# ════════════════════════════════════════════════════════════════════════════════

_SANCTIONS_TIMEOUT = 20
_SANCTIONS_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; ScreeningApp/4.0; Compliance)',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
}

# ── URL DG Trésor (lien permanent, toujours à jour) ──────────────────────────
_DGTRESOR_SANCTIONS_URL = "https://www.tresor.economie.gouv.fr/services-aux-entreprises/sanctions-economiques"

# ── FATF — base URL + calendrier des plénières (fév / juin / oct) ────────────
_FATF_BASE   = "https://www.fatf-gafi.org/en/publications/High-risk-and-other-monitored-jurisdictions"
_FATF_PLENARIES = [(2, "february"), (6, "june"), (10, "october")]

# ── Référentiel pays : clé → { label, iso2, variations } ─────────────────────
_COUNTRY_VARIATIONS: Dict[str, Dict] = {
    "russie":           {"label": "Russie",                    "iso2": "RU", "variations": ["russe", "russia", "russian", "russie"]},
    "bielorussie":      {"label": "Biélorussie",               "iso2": "BY", "variations": ["bielorusse", "belarus", "belarusian", "biélorussie", "bielorussie"]},
    "coree_nord":       {"label": "Corée du Nord (RPDC)",      "iso2": "KP", "variations": ["nord-coréen", "nord-coréenne", "north korean", "coree du nord", "corée du nord", "dprk", "rpdc", "north korea"]},
    "iran":             {"label": "Iran",                      "iso2": "IR", "variations": ["iranien", "iranienne", "iran", "iranian"]},
    "syrie":            {"label": "Syrie",                     "iso2": "SY", "variations": ["syrien", "syrienne", "syria", "syrian", "syrie"]},
    "myanmar":          {"label": "Myanmar (Birmanie)",        "iso2": "MM", "variations": ["birman", "birmane", "myanmar", "burmese", "birmanie"]},
    "venezuela":        {"label": "Venezuela",                 "iso2": "VE", "variations": ["vénézuélien", "vénézuélienne", "venezuelan", "venezuela"]},
    "zimbabwe":         {"label": "Zimbabwe",                  "iso2": "ZW", "variations": ["zimbabwéen", "zimbabwéenne", "zimbabwean", "zimbabwe"]},
    "nicaragua":        {"label": "Nicaragua",                 "iso2": "NI", "variations": ["nicaraguayen", "nicaraguayenne", "nicaraguan", "nicaragua"]},
    "soudan":           {"label": "Soudan",                    "iso2": "SD", "variations": ["soudanais", "soudanaise", "sudanese", "soudan", "sudan"]},
    "soudan_sud":       {"label": "Soudan du Sud",             "iso2": "SS", "variations": ["sud-soudanais", "south sudanese", "soudan du sud", "south sudan"]},
    "yemen":            {"label": "Yémen",                     "iso2": "YE", "variations": ["yéménite", "yemeni", "yemen", "yémen"]},
    "libye":            {"label": "Libye",                     "iso2": "LY", "variations": ["libyen", "libyenne", "libyan", "libya", "libye"]},
    "somalie":          {"label": "Somalie",                   "iso2": "SO", "variations": ["somalien", "somalienne", "somali", "somalian", "somalie"]},
    "rca":              {"label": "République centrafricaine", "iso2": "CF", "variations": ["centrafricain", "centrafricaine", "central african", "rca", "republique centrafricaine"]},
    "rdc":              {"label": "Rép. dém. du Congo",        "iso2": "CD", "variations": ["congolais", "congolaise", "congolese", "rdc", "rd congo", "republique democratique du congo"]},
    "mali":             {"label": "Mali",                      "iso2": "ML", "variations": ["malien", "malienne", "malian", "mali"]},
    "haiti":            {"label": "Haïti",                     "iso2": "HT", "variations": ["haïtien", "haïtienne", "haitian", "haiti", "haïti"]},
    "liban":            {"label": "Liban",                     "iso2": "LB", "variations": ["libanais", "libanaise", "lebanese", "lebanon", "liban"]},
    "irak":             {"label": "Irak",                      "iso2": "IQ", "variations": ["irakien", "irakienne", "iraqi", "irak", "iraq"]},
    "burundi":          {"label": "Burundi",                   "iso2": "BI", "variations": ["burundais", "burundaise", "burundian", "burundi"]},
    "guinee":           {"label": "Guinée",                    "iso2": "GN", "variations": ["guinéen", "guinéenne", "guinean", "guinee", "guinée"]},
    "guinee_bissau":    {"label": "Guinée-Bissau",             "iso2": "GW", "variations": ["bissau-guinéen", "guinea-bissau", "guinee-bissau"]},
    "niger":            {"label": "Niger",                     "iso2": "NE", "variations": ["nigérien", "nigérienne", "nigerien", "niger"]},
    "moldavie":         {"label": "Moldavie",                  "iso2": "MD", "variations": ["moldave", "moldovan", "moldova", "moldavie"]},
    "tunisie":          {"label": "Tunisie",                   "iso2": "TN", "variations": ["tunisien", "tunisienne", "tunisian", "tunisie", "tunisia"]},
    "turquie":          {"label": "Turquie",                   "iso2": "TR", "variations": ["turc", "turque", "turkish", "turquie", "turkey"]},
    "guatemala":        {"label": "Guatemala",                 "iso2": "GT", "variations": ["guatémaltèque", "guatemalan", "guatemala"]},
    "algerie":          {"label": "Algérie",                   "iso2": "DZ", "variations": ["algérien", "algérienne", "algerian", "algerie", "algérie", "algeria"]},
    "angola":           {"label": "Angola",                    "iso2": "AO", "variations": ["angolais", "angolaise", "angolan", "angola"]},
    "cote_ivoire":      {"label": "Côte d'Ivoire",             "iso2": "CI", "variations": ["ivoirien", "ivoirienne", "ivorian", "cote d'ivoire", "côte d'ivoire", "ivory coast"]},
    "kenya":            {"label": "Kenya",                     "iso2": "KE", "variations": ["kenyan", "kenyane", "kenya"]},
    "laos":             {"label": "Laos",                      "iso2": "LA", "variations": ["laotien", "laotienne", "lao", "laos"]},
    "nepal":            {"label": "Népal",                     "iso2": "NP", "variations": ["népalais", "népalaise", "nepalese", "nepal", "népal"]},
    "cameroun":         {"label": "Cameroun",                  "iso2": "CM", "variations": ["camerounais", "camerounaise", "cameroonian", "cameroun", "cameroon"]},
    "philippines":      {"label": "Philippines",               "iso2": "PH", "variations": ["philippin", "philippine", "filipino", "filipina", "philippines"]},
    "senegal":          {"label": "Sénégal",                   "iso2": "SN", "variations": ["sénégalais", "sénégalaise", "senegalese", "senegal", "sénégal"]},
    "vietnam":          {"label": "Vietnam",                   "iso2": "VN", "variations": ["vietnamien", "vietnamienne", "vietnamese", "vietnam", "viet nam"]},
    "namibie":          {"label": "Namibie",                   "iso2": "NA", "variations": ["namibien", "namibienne", "namibian", "namibie", "namibia"]},
    "bolivie":          {"label": "Bolivie",                   "iso2": "BO", "variations": ["bolivien", "bolivienne", "bolivian", "bolivie", "bolivia"]},
    "iles_vierges_brit":{"label": "Îles Vierges brit.",        "iso2": "VG", "variations": ["british virgin islands", "iles vierges britanniques", "bvi"]},
    "mozambique":       {"label": "Mozambique",                "iso2": "MZ", "variations": ["mozambicain", "mozambicaine", "mozambican", "mozambique"]},
    "bulgarie":         {"label": "Bulgarie",                  "iso2": "BG", "variations": ["bulgare", "bulgarian", "bulgarie", "bulgaria"]},
    "mongolie":         {"label": "Mongolie",                  "iso2": "MN", "variations": ["mongol", "mongole", "mongolian", "mongolie", "mongolia"]},
    "koweit":           {"label": "Koweït",                    "iso2": "KW", "variations": ["koweïtien", "koweïtienne", "kuwaiti", "koweit", "koweït", "kuwait"]},
    "papouasie":        {"label": "Papouasie-Nouvelle-Guinée", "iso2": "PG", "variations": ["papouasien", "papouasienne", "papua new guinea", "papouasie", "png"]},
}

# ── Fallback statique FATF — plénière février 2026 ────────────────────────────
_FATF_STATIC = {
    "black": ["KP", "IR", "MM"],
    "grey":  ["DZ","AO","BO","BG","CM","CI","CD","HT","KE","KW",
               "LA","LB","LY","MN","NP","NI","PG","SN","SO","SS",
               "SY","TR","VE","VN","YE","VG","NE"],
}

# ── Fallback statique DG Trésor — scrappé le 21/04/2026 ──────────────────────
_DGTRESOR_STATIC = ["BY","BI","CD","KP","GT","GW","GN","HT","IR","IQ",
                    "LB","LY","ML","MD","MM","NI","NE","CF","RU","SO",
                    "SD","SS","SY","TN","TR","VE","YE","ZW"]

# ── Table nom DG Trésor → ISO2 ────────────────────────────────────────────────
_DGTRESOR_NAME_TO_ISO2 = {
    "biélorussie":"BY","burundi":"BI","congo (république démocratique du)":"CD",
    "corée du nord":"KP","guatemala":"GT","guinée-bissau":"GW","guinée":"GN",
    "haïti":"HT","iran":"IR","irak":"IQ","liban":"LB","libye":"LY",
    "mali":"ML","moldavie":"MD","myanmar (ex-birmanie)":"MM","nicaragua":"NI",
    "niger":"NE","république centrafricaine":"CF","russie":"RU","somalie":"SO",
    "soudan":"SD","soudan du sud":"SS","syrie":"SY","tunisie":"TN",
    "turquie":"TR","venezuela":"VE","yémen":"YE","zimbabwe":"ZW",
}

# ── Table nom FATF → ISO2 ─────────────────────────────────────────────────────
_FATF_NAME_TO_ISO2 = {
    "democratic people's republic of korea":"KP","north korea":"KP","dprk":"KP",
    "iran":"IR","myanmar":"MM","burma":"MM",
    "algeria":"DZ","angola":"AO","bolivia":"BO","bulgaria":"BG",
    "cameroon":"CM","côte d'ivoire":"CI","cote d'ivoire":"CI","ivory coast":"CI",
    "democratic republic of the congo":"CD","drc":"CD",
    "haiti":"HT","kenya":"KE","kuwait":"KW",
    "laos":"LA","lao pdr":"LA","lao people's democratic republic":"LA",
    "lebanon":"LB","libya":"LY","mongolia":"MN",
    "nepal":"NP","nicaragua":"NI","niger":"NE",
    "papua new guinea":"PG","png":"PG",
    "senegal":"SN","somalia":"SO","south sudan":"SS",
    "syria":"SY","turkey":"TR",
    "venezuela":"VE","vietnam":"VN","yemen":"YE",
    "british virgin islands":"VG",
}

# ── Métadonnées par niveau de risque ─────────────────────────────────────────
_RISK_METADATA = {
    "LISTE_NOIRE":       {"action":"CONTRE_MESURES",    "source_label":"FATF – Liste noire (appel à l'action)",             "rationale":"Défaillances stratégiques graves LBC/FT/FP. Contre-mesures obligatoires."},
    "SANCTIONS_UE":      {"action":"DDR_RENFORCEE",     "source_label":"DG Trésor / Règlement UE – Sanctions géographiques","rationale":"Pays soumis à un régime de sanctions économiques et financières UE/ONU en vigueur sur le territoire français."},
    "LISTE_GRISE":       {"action":"VIGILANCE_RENFORCEE","source_label":"FATF – Liste grise (surveillance renforcée)",      "rationale":"Défaillances stratégiques LBC/FT. Travaille avec le FATF sur un plan d'action correctif."},
    "SANCTIONS_ET_GRISE":{"action":"DDR_RENFORCEE",     "source_label":"DG Trésor + FATF liste grise",                     "rationale":"Pays sous sanctions UE/ONU ET sous surveillance renforcée FATF."},
}


def _sanctions_norm(text: str) -> str:
    text = str(text).lower()
    text = unicodedata.normalize("NFKD", text)
    return "".join(c for c in text if not unicodedata.combining(c))


def _get_fatf_urls() -> Tuple[str, str, str]:
    """Détecte dynamiquement les URLs de la dernière plénière FATF publiée."""
    now = datetime.now()
    candidates = []
    for year in range(now.year, now.year - 2, -1):
        for month, month_name in reversed(_FATF_PLENARIES):
            if year == now.year and month > now.month:
                continue
            candidates.append((year, month, month_name))

    for year, month, month_name in candidates:
        black_url = f"{_FATF_BASE}/Call-for-action-{month_name}-{year}.html"
        grey_url  = f"{_FATF_BASE}/increased-monitoring-{month_name}-{year}.html"
        label     = f"{month_name.capitalize()} {year}"
        try:
            r = requests.head(black_url, timeout=8, headers=_SANCTIONS_HEADERS, allow_redirects=True)
            if r.status_code == 200:
                print(f"   ✅ FATF URLs détectées : {label}")
                return black_url, grey_url, label
        except Exception:
            pass

    print("   ⚠️  FATF URL auto-détection échouée — fallback février 2026")
    return (f"{_FATF_BASE}/Call-for-action-february-2026.html",
            f"{_FATF_BASE}/increased-monitoring-february-2026.html",
            "February 2026 (fallback)")


def _scrape_dgtresor() -> List[str]:
    try:
        resp = requests.get(_DGTRESOR_SANCTIONS_URL, timeout=_SANCTIONS_TIMEOUT, headers=_SANCTIONS_HEADERS)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        iso2_list = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            if "sanctions-economiques/" not in href:
                continue
            if any(x in href for x in ["regime-ue", "regime-onu", "lutte-contre", "russie-en-lien"]):
                if "russie" in href:
                    iso2_list.append("RU")
                continue
            text_norm = _sanctions_norm(a_tag.get_text(strip=True))
            if text_norm in _DGTRESOR_NAME_TO_ISO2:
                iso2 = _DGTRESOR_NAME_TO_ISO2[text_norm]
                if iso2 not in iso2_list:
                    iso2_list.append(iso2)
        if not iso2_list:
            print("   ⚠️  DG Trésor : aucun pays extrait — fallback statique")
            return list(_DGTRESOR_STATIC)
        print(f"   ✅ DG Trésor : {len(iso2_list)} pays sous sanctions")
        return iso2_list
    except Exception as e:
        print(f"   ⚠️  DG Trésor scraping échoué ({e}) — fallback statique")
        return list(_DGTRESOR_STATIC)


def _scrape_fatf_page(url: str, list_type: str) -> List[str]:
    try:
        resp = requests.get(url, timeout=_SANCTIONS_TIMEOUT, headers=_SANCTIONS_HEADERS)
        resp.raise_for_status()
        full_text = BeautifulSoup(resp.text, "html.parser").get_text(" ", strip=True).lower()
        iso2_list = [iso2 for name, iso2 in _FATF_NAME_TO_ISO2.items()
                     if name in full_text]
        # Dédupliquer en conservant l'ordre
        seen = set(); iso2_list = [x for x in iso2_list if not (x in seen or seen.add(x))]
        print(f"   {'✅' if iso2_list else '⚠️ '} FATF {list_type} : {len(iso2_list)} pays")
        return iso2_list
    except Exception as e:
        print(f"   ⚠️  FATF {list_type} scraping échoué ({e})")
        return []


def _build_risk_db(dgtresor: List[str], black: List[str], grey: List[str]) -> Dict:
    risk_db = {}
    for key, info in _COUNTRY_VARIATIONS.items():
        iso2 = info.get("iso2", "")
        in_black = iso2 in black
        in_dgt   = iso2 in dgtresor
        in_grey  = iso2 in grey
        if not (in_black or in_dgt or in_grey):
            continue
        if in_black:
            level = "LISTE_NOIRE"
        elif in_dgt and in_grey:
            level = "SANCTIONS_ET_GRISE"
        elif in_dgt:
            level = "SANCTIONS_UE"
        else:
            level = "LISTE_GRISE"
        meta = _RISK_METADATA[level]
        risk_db[key] = {**info, "risk_level": level, "action": meta["action"],
                        "source": meta["source_label"], "rationale": meta["rationale"],
                        "in_fatf_black": in_black, "in_fatf_grey": in_grey, "in_dgtresor": in_dgt}
    return risk_db


class SanctionsLoader:
    """Charge et met en cache les listes de pays à risque (DG Trésor + FATF). TTL 24h."""
    TTL_HOURS = 24

    def __init__(self):
        self.risk_db: Dict         = {}
        self.last_loaded: Optional[datetime] = None
        self.fatf_label: str       = ""
        self.dgtresor_count: int   = 0
        self.fatf_black_count: int = 0
        self.fatf_grey_count: int  = 0
        self.source_status: Dict   = {}

    def is_stale(self) -> bool:
        return not self.risk_db or self.last_loaded is None or \
               datetime.now() - self.last_loaded > timedelta(hours=self.TTL_HOURS)

    def load(self, force: bool = False) -> bool:
        if not force and not self.is_stale():
            return True
        print("\n" + "="*60)
        print("🔄 CHARGEMENT LISTES DE SANCTIONS (DG Trésor + FATF)")
        print("="*60)

        print("\n🇫🇷 DG Trésor — Sanctions géographiques…")
        dgtresor = _scrape_dgtresor()
        self.dgtresor_count = len(dgtresor)
        self.source_status["dgtresor"] = "✅ Live" if dgtresor != _DGTRESOR_STATIC else "⚠️ Fallback"

        print("\n🌐 FATF — Détection + scraping…")
        black_url, grey_url, self.fatf_label = _get_fatf_urls()
        black = _scrape_fatf_page(black_url, "noire") or list(_FATF_STATIC["black"])
        grey  = _scrape_fatf_page(grey_url,  "grise") or list(_FATF_STATIC["grey"])
        self.fatf_black_count = len(black)
        self.fatf_grey_count  = len(grey)
        self.source_status["fatf"] = "✅ Live" if (black != _FATF_STATIC["black"] or grey != _FATF_STATIC["grey"]) else "⚠️ Fallback"

        self.risk_db = _build_risk_db(dgtresor, black, grey)
        self.last_loaded = datetime.now()
        print(f"\n✅ {len(self.risk_db)} pays à risque chargés — FATF : {self.fatf_label}")
        print("="*60 + "\n")
        return True

    def get_nationality_risk(self, nationality: str) -> Optional[Dict]:
        if self.is_stale():
            self.load()
        if not nationality or not self.risk_db:
            return None
        nat_norm = _sanctions_norm(nationality)
        nat_tokens = set(nat_norm.split())

        for key, info in self.risk_db.items():
            for v in info["variations"]:
                v_norm = _sanctions_norm(v)
                if not v_norm:
                    continue
                v_tokens = set(v_norm.split())

                # Match exact sur la saisie complète
                if nat_norm == v_norm:
                    matched = True
                # La variation est un mot seul → doit correspondre exactement à un token de la saisie
                elif len(v_tokens) == 1:
                    matched = v_norm in nat_tokens
                # La variation est multi-mots → la saisie doit la contenir exactement
                else:
                    matched = v_norm in nat_norm

                if matched:
                    return {**{k: info[k] for k in ("label","iso2","risk_level","action","source","rationale",
                                                     "in_fatf_black","in_fatf_grey","in_dgtresor")},
                            "country_key": key,
                            "fatf_label":  self.fatf_label,
                            "loaded_at":   self.last_loaded.strftime("%d/%m/%Y à %H:%M") if self.last_loaded else "?"}
        return None

    def get_status_info(self) -> Dict:
        return {
            "last_loaded":          self.last_loaded.strftime("%d/%m/%Y à %H:%M") if self.last_loaded else "Jamais",
            "next_reload":          (self.last_loaded + timedelta(hours=self.TTL_HOURS)).strftime("%d/%m/%Y à %H:%M") if self.last_loaded else "—",
            "fatf_label":           self.fatf_label,
            "dgtresor_count":       self.dgtresor_count,
            "fatf_black_count":     self.fatf_black_count,
            "fatf_grey_count":      self.fatf_grey_count,
            "total_risk_countries": len(self.risk_db),
            "dgtresor_status":      self.source_status.get("dgtresor", "—"),
            "fatf_status":          self.source_status.get("fatf", "—"),
            "is_stale":             self.is_stale(),
        }

    def force_reload(self) -> bool:
        return self.load(force=True)


# Singleton interne — accès via get_sanctions_loader()
_sanctions_loader_instance: Optional[SanctionsLoader] = None


def get_sanctions_loader() -> SanctionsLoader:
    global _sanctions_loader_instance
    if _sanctions_loader_instance is None:
        _sanctions_loader_instance = SanctionsLoader()
        _sanctions_loader_instance.load()
    elif _sanctions_loader_instance.is_stale():
        _sanctions_loader_instance.load()
    return _sanctions_loader_instance


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 2 — MOTEUR GDA / PPE
# ════════════════════════════════════════════════════════════════════════════════


def norm(text: str) -> str:
    """Normalisation utilisée par le moteur GDA/PPE."""
    if not text:
        return ""
    text = str(text).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text


class ScreeningEngine:

    def __init__(self):
        self.entries    = []
        self.last_update = None
        self.source     = None          # "France" ou "Monaco"
        self.source_url = None          # URL effectivement utilisée

        print("\n" + "="*70)
        print("🔄 CHARGEMENT DONNÉES GDA — tentative France en premier")
        print("="*70)

        if self.load_from_france():
            print(f"\n✅ {len(self.entries)} entrées chargées (source : France)")
        else:
            print("\n⚠️  France indisponible — bascule sur Monaco…")
            if self.load_from_monaco():
                print(f"\n✅ {len(self.entries)} entrées chargées (source : Monaco)")
            else:
                print("\n❌ Échec des deux sources — aucune donnée disponible")

        print("="*70 + "\n")

    # ── Source primaire : France ─────────────────────────────────────────────

    def load_from_france(self) -> bool:
        """
        Charge le registre depuis l'API officielle DG Trésor (France).
        Endpoint : derniere-publication-flux-json
        Structure : { "Publications": { "DatePublication": "...",
                                        "PublicationDetail": [ { "Nom": ...,
                                          "Nature": ...,
                                          "RegistreDetail": [ {"TypeChamp": ..., "Valeur": [...]} ]
                                        } ] } }
        """
        try:
            print(f"🇫🇷 Tentative API France (timeout {FRANCE_TIMEOUT}s)…")

            response = requests.get(
                FRANCE_JSON_URL,
                timeout=FRANCE_TIMEOUT,
                headers={
                    'User-Agent': 'Mozilla/5.0 (compatible; ScreeningApp/4.0)',
                    'Accept': 'application/json',
                },
            )

            print(f"   Status : {response.status_code}")

            if response.status_code != 200:
                print(f"   ❌ Code HTTP inattendu ({response.status_code})")
                return False

            print(f"   Taille : {len(response.content) / 1024:.1f} KB")

            data = response.json()

            # Structure réelle de l'API DG Trésor
            pub     = data.get("Publications", {})
            items   = pub.get("PublicationDetail", [])
            date_pub = pub.get("DatePublication", "")

            count = 0
            for item in items:
                nom    = str(item.get("Nom", "")).strip()
                nature = str(item.get("Nature", ""))

                # On ne retient que les personnes physiques
                if nature != "Personne physique":
                    continue

                # ── Mapping exact du schéma JSON officiel DG Trésor ─────────
                # Champs top-level : IdRegistre | Nature | Nom | RegistreDetail
                # TypeChamp possibles (Personne physique) :
                #   PRENOM | ALIAS | SEXE | DATE_DE_NAISSANCE | LIEU_DE_NAISSANCE
                #   NATIONALITE | TITRE | ADRESSE_PP | AUTRE_IDENTITE | PASSEPORT
                #   CRYPTOMMONNAIE | MOTIFS | FONDEMENT_JURIDIQUE | REFERENCE_UE | REFERENCE_ONU

                prenoms        = []
                nationalities  = []
                dob            = ""
                alias          = []          # Alias.Alias (string)
                lieu_naissance = ""          # LieuDeNaissance.Lieu + .Pays
                titre          = ""          # Titre.Titre
                adresse        = []          # Adresse.Adresse + .Pays  (TypeChamp=ADRESSE_PP)
                passeport      = []          # Passeport.NumeroPasseport + .Commentaire
                identification = []          # AutreIdentite.NumeroCarte + .Commentaire
                fondement_jur  = []          # FondementJuridique.FondementJuridiqueLabel
                ref_ue         = []          # ReferenceUe.ReferenceUe
                ref_onu        = []          # ReferenceOnu.ReferenceOnu
                motifs         = []          # Motifs.Motifs (string)
                regime         = str(item.get("Regime", "")).strip()
                id_registre    = str(item.get("IdRegistre", "")).strip()

                for detail in item.get("RegistreDetail", []):
                    type_champ = detail.get("TypeChamp", "")
                    valeurs    = detail.get("Valeur") or []

                    # ── PRENOM → Prenom.Prenom ───────────────────────────────
                    if type_champ == "PRENOM":
                        prenoms += [v.get("Prenom", "") for v in valeurs if v.get("Prenom")]

                    # ── ALIAS → Alias.Alias (+ Commentaire optionnel) ────────
                    elif type_champ == "ALIAS":
                        for v in valeurs:
                            a = v.get("Alias", "").strip()
                            commentaire = v.get("Commentaire", "").strip()
                            if a:
                                alias.append(f"{a} ({commentaire})" if commentaire else a)

                    # ── DATE_DE_NAISSANCE → Annee/Mois/Jour (+ Commentaire) ──
                    elif type_champ == "DATE_DE_NAISSANCE":
                        v = valeurs[0] if valeurs else {}
                        y = v.get("Annee", "")
                        m = v.get("Mois", "")
                        j = v.get("Jour", "")
                        commentaire = v.get("Commentaire", "")
                        if y:
                            if m and j:
                                dob = f"{y}-{str(m).zfill(2)}-{str(j).zfill(2)}"
                            elif m:
                                dob = f"{y}-{str(m).zfill(2)}"
                            else:
                                dob = str(y)
                            if commentaire:
                                dob += f" ({commentaire})"

                    # ── LIEU_DE_NAISSANCE → Lieu + Pays ─────────────────────
                    elif type_champ == "LIEU_DE_NAISSANCE":
                        v = valeurs[0] if valeurs else {}
                        lieu = v.get("Lieu", "").strip()   # clé exacte du schéma : "Lieu"
                        pays = v.get("Pays", "").strip()
                        if lieu and pays:
                            lieu_naissance = f"{lieu} ({pays})"
                        elif lieu:
                            lieu_naissance = lieu
                        elif pays:
                            lieu_naissance = pays

                    # ── NATIONALITE → Pays (+ Commentaire) ──────────────────
                    elif type_champ == "NATIONALITE":
                        for v in valeurs:
                            pays = v.get("Pays", "").strip()
                            commentaire = v.get("Commentaire", "").strip()
                            if pays:
                                nationalities.append(
                                    f"{pays} ({commentaire})" if commentaire else pays
                                )

                    # ── TITRE → Titre.Titre ──────────────────────────────────
                    elif type_champ == "TITRE":
                        titres = [v.get("Titre", "") for v in valeurs if v.get("Titre")]
                        if titres:
                            titre = " | ".join(titres)

                    # ── ADRESSE_PP → Adresse + Pays ──────────────────────────
                    elif type_champ == "ADRESSE_PP":
                        for v in valeurs:
                            parts = [v.get("Adresse", ""), v.get("Pays", "")]
                            addr = ", ".join(p for p in parts if p)
                            if addr:
                                adresse.append(addr)

                    # ── PASSEPORT → NumeroPasseport (+ Commentaire) ──────────
                    elif type_champ == "PASSEPORT":
                        for v in valeurs:
                            num = v.get("NumeroPasseport", "").strip()
                            commentaire = v.get("Commentaire", "").strip()
                            if num:
                                passeport.append(
                                    f"{num} ({commentaire})" if commentaire else num
                                )

                    # ── AUTRE_IDENTITE → NumeroCarte (+ Commentaire) ─────────
                    elif type_champ == "AUTRE_IDENTITE":
                        for v in valeurs:
                            num = v.get("NumeroCarte", "").strip()
                            commentaire = v.get("Commentaire", "").strip()
                            if num:
                                identification.append(
                                    f"{num} ({commentaire})" if commentaire else num
                                )

                    # ── MOTIFS → Motifs.Motifs ───────────────────────────────
                    elif type_champ == "MOTIFS":
                        for v in valeurs:
                            m_txt = v.get("Motifs", "").strip()   # clé exacte : "Motifs"
                            if m_txt:
                                motifs.append(m_txt)

                    # ── FONDEMENT_JURIDIQUE → FondementJuridiqueLabel ────────
                    elif type_champ == "FONDEMENT_JURIDIQUE":
                        for v in valeurs:
                            label = v.get("FondementJuridiqueLabel", "").strip()
                            if label:
                                fondement_jur.append(label)

                    # ── REFERENCE_UE → ReferenceUe.ReferenceUe ──────────────
                    elif type_champ == "REFERENCE_UE":
                        for v in valeurs:
                            ref = v.get("ReferenceUe", "").strip()
                            if ref:
                                ref_ue.append(ref)

                    # ── REFERENCE_ONU → ReferenceOnu.ReferenceOnu ───────────
                    elif type_champ == "REFERENCE_ONU":
                        for v in valeurs:
                            ref = v.get("ReferenceOnu", "").strip()
                            if ref:
                                ref_onu.append(ref)

                if not nom and not prenoms:
                    continue

                # Consolider fondement juridique + références UE/ONU
                fondement_complet = " | ".join(filter(None, fondement_jur + ref_ue + ref_onu))

                # Une entrée par prénom
                for prenom in (prenoms if prenoms else [""]):
                    premier = prenom.split()[0] if prenom else ""
                    self.entries.append({
                        # ── Identité ────────────────────────────────────────
                        'id_registre':     id_registre,
                        'regime':          regime,
                        'nom':             nom,
                        'prenom':          premier,
                        'prenom_complet':  prenom,
                        'alias':           [a for a in alias if a],
                        'titre':           titre,
                        # ── Naissance ───────────────────────────────────────
                        'date_naissance':  dob,
                        'lieu_naissance':  lieu_naissance,
                        # ── Nationalité ─────────────────────────────────────
                        'nationalite':     [n for n in nationalities if n],
                        # ── Documents ───────────────────────────────────────
                        'passeport':       passeport,
                        'identification':  identification,
                        # ── Localisation ────────────────────────────────────
                        'adresse':         adresse,
                        # ── Sanctions ───────────────────────────────────────
                        'motifs':          " ".join(motifs),         # Motifs.Motifs
                        'fondement_jur':   fondement_complet,        # FondementJuridiqueLabel + ReferenceUe + ReferenceOnu
                    })
                    count += 1

            if count == 0:
                print("   ⚠️  Aucune personne physique extraite du JSON France")
                return False

            print(f"   ✅ {count} personnes physiques (France)")

            # Date de dernière publication
            if date_pub:
                self.last_update = str(date_pub)[:16].replace("T", " à ").replace("-", "/")
            else:
                try:
                    r = requests.get(FRANCE_DATE_URL, timeout=10)
                    if r.ok:
                        self.last_update = (r.json().get("datePublication") or r.text.strip())[:16]
                except Exception:
                    pass

            self.source     = "France"
            self.source_url = FRANCE_JSON_URL
            return True

        except requests.exceptions.Timeout:
            print(f"   ⏱️  Timeout après {FRANCE_TIMEOUT}s — API France non disponible")
            return False
        except requests.exceptions.ConnectionError as e:
            print(f"   🔌 Connexion impossible à l'API France : {e}")
            return False
        except Exception as e:
            print(f"   ❌ Erreur inattendue (France) : {e}")
            return False

    # ── Source de repli : Monaco ─────────────────────────────────────────────

    def load_from_monaco(self) -> bool:
        """
        Charge le registre depuis l'API officielle de Monaco.
        Utilisé uniquement si l'API France a échoué.
        """
        try:
            print(f"🇲🇨 Tentative API Monaco (timeout {MONACO_TIMEOUT}s)…")

            response = requests.get(
                MONACO_JSON_URL,
                timeout=MONACO_TIMEOUT,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json',
                    'Referer': 'https://geldefonds.gouv.mc/',
                },
            )

            print(f"   Status : {response.status_code}")

            if response.status_code != 200:
                print(f"   ❌ Code HTTP inattendu ({response.status_code})")
                return False

            print(f"   Taille : {len(response.content) / 1024:.1f} KB")

            data = response.json()
            measures = data if isinstance(data, list) else []

            count = 0
            for m in measures:
                if m.get('nature') == 'Personne physique':
                    nom     = str(m.get('nom', '')).strip()
                    details = m.get('mesureDetails', {})
                    prenom  = str(details.get('prenom', '')).strip()
                    date    = str(details.get('dateNaissance', '')).strip()
                    nat     = str(details.get('nationalite', '')).strip()
                    alias_raw = details.get('alias', [])
                    alias   = [str(a) for a in alias_raw] if isinstance(alias_raw, list) else []
                    # Colonnes registre : Motifs | Fondement juridique | Régime | Titre | Adresse | Passeport
                    motifs        = str(m.get('motif', '') or m.get('motifs', '') or m.get('commentaire', '')).strip()
                    fondement_jur = str(m.get('reference', '') or m.get('reglementRef', '') or m.get('fondementJuridique', '')).strip()
                    regime        = str(m.get('regime', '')).strip()
                    titre         = str(details.get('titre', '')).strip()
                    lieu_naiss    = str(details.get('lieuNaissance', '')).strip()
                    adresse_raw   = details.get('adresse', '')
                    adresse       = [str(adresse_raw)] if adresse_raw else []
                    passeport_raw = details.get('passeport', '')
                    passeport     = [str(passeport_raw)] if passeport_raw else []

                    if nom or prenom:
                        premier = prenom.split()[0] if prenom else ""
                        self.entries.append({
                            'id_registre':    str(m.get('id', '')),
                            'regime':         regime,
                            'nom':            nom,
                            'prenom':         premier,
                            'prenom_complet': prenom,
                            'alias':          [a for a in alias if a],
                            'titre':          titre,
                            'date_naissance': date,
                            'lieu_naissance': lieu_naiss,
                            'nationalite':    [nat] if nat else [],
                            'passeport':      passeport,
                            'identification': [],
                            'adresse':        adresse,
                            'fondement_jur':  fondement_jur,
                            'motifs':         motifs,
                        })
                        count += 1

            if count == 0:
                print("   ⚠️  Aucune personne physique extraite du JSON Monaco")
                return False

            print(f"   ✅ {count} personnes physiques (Monaco)")

            # Date de dernière publication
            try:
                r = requests.get(MONACO_DATE_URL, timeout=10)
                if r.status_code == 200:
                    txt = r.text.strip().strip('"')
                    if '-' in txt:
                        parts = txt.split(' ')
                        d = parts[0].replace('-', '/')
                        t = parts[1][:5] if len(parts) > 1 else ''
                        self.last_update = f"{d} à {t}"
            except Exception:
                pass

            self.source     = "Monaco"
            self.source_url = MONACO_JSON_URL
            return True

        except requests.exceptions.Timeout:
            print(f"   ⏱️  Timeout après {MONACO_TIMEOUT}s — API Monaco non disponible")
            return False
        except requests.exceptions.ConnectionError as e:
            print(f"   🔌 Connexion impossible à l'API Monaco : {e}")
            return False
        except Exception as e:
            print(f"   ❌ Erreur inattendue (Monaco) : {e}")
            return False

    def get_last_update_info(self) -> str:
        flag = "🇫🇷" if self.source == "France" else "🇲🇨"
        src  = self.source or "Inconnue"
        return f"{self.last_update} ({flag} {src})" if self.last_update else f"{flag} {src}"

    def is_ready(self) -> bool:
        return len(self.entries) > 0

    def detect_ppe_risk(self, client_data: Dict[str, Any]) -> Tuple[bool, int, List[str], str]:
        profession = norm(client_data.get("profession", ""))

        if not profession:
            return False, 0, [], ""

        risk_score = 0
        matched = []

        # ÉTAPE 1: Vérifier les mots PPE clairs AVANT les ambigus
        for cat, kws in PPE_KEYWORDS.items():
            for kw in kws:
                kw_norm = norm(kw)
                if kw_norm in profession:
                    # Calculer le score selon la catégorie (aligné avec PPE_KEYWORDS)
                    if cat in ["président", "ministre", "premier_ministre"]:
                        score_boost = 25  # Niveau 1
                    elif cat in ["député", "sénateur", "ambassadeur", "général", "juge", "procureur"]:
                        score_boost = 20  # Niveau 2
                    elif cat in ["maire", "préfet", "gouverneur", "consul", "colonel", "commissaire",
                                 "directeur_agence", "président_conseil"]:
                        score_boost = 15  # Niveau 3
                    else:
                        score_boost = 10  # Niveau 4

                    risk_score += score_boost
                    matched.append(kw)

        # ÉTAPE 2: Mots ambigus (seulement si aucun PPE clair détecté)
        if risk_score == 0:
            mots_ambigus = ["directeur", "director", "president", "chairman", "dg", "ceo"]

            for mot_ambigu in mots_ambigus:
                if norm(mot_ambigu) in profession:
                    # Vérifier contexte
                    contexte_prive = ["entreprise", "societe", "société", "company", "business", "startup", "privé", "private"]
                    contexte_public = ["ministere", "ministère", "ministry", "etat", "état", "gouvernement",
                                       "government", "public", "administration", "mairie", "region", "région"]

                    has_private = any(norm(c) in profession for c in contexte_prive)
                    has_public = any(norm(c) in profession for c in contexte_public)

                    # Si pas de contexte → clarification
                    if not has_private and not has_public:
                        return False, 0, [], "CLARIFICATION_REQUISE"
                    elif has_public and not has_private:
                        # Public → PPE
                        risk_score += 20
                        matched.append(mot_ambigu)

        return risk_score >= 15, risk_score, matched, ""

    def check_gda_live(self, first_name: str, last_name: str) -> Tuple[bool, int, str]:
        """
        DÉTECTION EN TEMPS RÉEL pendant la saisie
        Supporte prénoms composés (ex: "Vyacheslav Nikolaevich")
        """
        if not first_name and not last_name:
            return False, 0, ""

        if not self.entries:
            return False, 0, ""

        first_norm = norm(first_name) if first_name else ""
        last_norm = norm(last_name) if last_name else ""

        # Chercher dans les entrées
        best_score = 0
        best_name = ""

        for entry in self.entries:
            # IMPORTANT: Vérifier à la fois le premier prénom ET le prénom complet
            entry_first = norm(entry.get('prenom', ''))  # Premier prénom
            entry_first_complet = norm(entry.get('prenom_complet', ''))  # Tous les prénoms
            entry_last = norm(entry.get('nom', ''))

            score = 0

            # Match prénom - vérifier contre TOUS les prénoms
            if first_norm:
                # Essayer avec premier prénom
                if entry_first and first_norm == entry_first:
                    score += 50
                elif entry_first and (first_norm in entry_first or entry_first in first_norm):
                    score += 40
                elif entry_first and len(first_norm) >= 3 and len(entry_first) >= 3 and first_norm[:3] == entry_first[:3]:
                    score += 30

                # Essayer aussi avec prénom complet (pour prénoms composés)
                if entry_first_complet:
                    # Si le prénom recherché est dans les prénoms composés
                    if first_norm in entry_first_complet:
                        score = max(score, 45)  # Bon match
                    # Vérifier chaque partie du prénom composé
                    for part in entry_first_complet.split():
                        if first_norm == part:
                            score = max(score, 50)  # Match exact sur une partie
                        elif first_norm in part or part in first_norm:
                            score = max(score, 40)

            # Match nom
            if last_norm and entry_last:
                if last_norm == entry_last:
                    score += 50
                elif last_norm in entry_last or entry_last in last_norm:
                    score += 40
                elif len(last_norm) >= 3 and len(entry_last) >= 3 and last_norm[:3] == entry_last[:3]:
                    score += 30

            if score > best_score:
                best_score = score
                # Afficher le prénom complet dans le message
                prenom_display = entry.get('prenom_complet', entry.get('prenom', ''))
                best_name = f"{prenom_display} {entry.get('nom', '')}".strip()

        # Retourner si score >= 60 (détection précoce)
        if best_score >= 60:
            return True, best_score, best_name

        return False, best_score, best_name

    def screen_client(self, client_data: Dict[str, Any],
                      is_public_sector: Optional[bool] = None,
                      ppe_answers: Optional[Dict[str, bool]] = None) -> Dict[str, Any]:

        first_name = client_data.get("first_name", "")
        last_name = client_data.get("last_name", "")

        # PPE
        is_ppe, ppe_score, ppe_kws, ppe_signal = self.detect_ppe_risk(client_data)

        # Si clarification requise et pas de réponse fournie
        if ppe_signal == "CLARIFICATION_REQUISE" and is_public_sector is None:
            return {
                "decision": "PENDING_CLARIFICATION",
                "score": 0,
                "gda_score": 0,
                "decision_reason": "Profession ambiguë : précisez si secteur public ou privé",
                "alert_type": "PPE_CLARIFICATION",
                "can_subscribe": False
            }

        # Si clarification fournie et secteur public confirmé
        elif ppe_signal == "CLARIFICATION_REQUISE" and is_public_sector is True:
            is_ppe = True
            ppe_score = 20
            ppe_kws = ["Directeur secteur public"]

        if ppe_signal == "CLARIFICATION_REQUISE" and is_public_sector is False:
            is_ppe = False
            ppe_score = 0
            ppe_kws = []

        # GDA avec RapidFuzz
        if not self.entries:
            return {"decision": "OK", "can_subscribe": True}

        entry_names_norm = []
        for entry in self.entries:
            name = f"{entry.get('prenom', '')} {entry.get('nom', '')}".strip()
            name_norm = norm(name)
            if name_norm:
                entry_names_norm.append((name_norm, entry))

        client_name = f"{first_name} {last_name}".strip()
        client_norm = norm(client_name)

        names_only = [n for n, _ in entry_names_norm]
        matches = process.extract(
            client_norm,
            names_only,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=30,
            limit=5
        )

        best_score = 0
        best_match = None
        matched_fields = []

        for matched_norm, score, _ in matches:
            for name_norm, entry in entry_names_norm:
                if name_norm == matched_norm:
                    total_score = score

                    # Bonus date
                    if client_data.get("birth_date") and entry.get("date_naissance"):
                        client_dob = client_data["birth_date"].replace("-", "/")
                        entry_dob = entry["date_naissance"].replace("-", "/")

                        if client_dob == entry_dob:
                            total_score += 20
                            matched_fields.append("birth_date")
                        elif len(client_dob) >= 4 and len(entry_dob) >= 4 and client_dob[-4:] == entry_dob[-4:]:
                            total_score += 5
                            matched_fields.append("birth_year")

                    # Bonus nationalité
                    if client_data.get("nationality") and entry.get("nationalite"):
                        client_nat = norm(client_data["nationality"])
                        entry_nats = [norm(n) for n in entry["nationalite"] if n]

                        if any(client_nat[:3] in n or n[:3] in client_nat
                               for n in entry_nats if len(n) >= 3 and len(client_nat) >= 3):
                            total_score += 10
                            matched_fields.append("nationality")

                    if total_score > best_score:
                        best_score = total_score
                        best_match = entry
                    break

        # Décision
        decision = "OK"
        reason = "Aucune correspondance"
        alert = "NONE"
        match_name = ""

        # Construire les détails enrichis du match GDA — colonnes exactes du registre
        gda_details = None
        if best_match and best_score >= SEUIL_REVIEW:
            prenom_display = best_match.get('prenom_complet', best_match.get('prenom', ''))
            match_name = f"{prenom_display} {best_match.get('nom', '')}".strip()

            gda_details = {
                # ── Identité ────────────────────────────────────────────────
                "id_registre":    best_match.get('id_registre', ''),
                "regime":         best_match.get('regime', ''),
                "nom_complet":    match_name,
                "alias":          best_match.get('alias', []),
                "titre":          best_match.get('titre', ''),
                # ── Naissance ───────────────────────────────────────────────
                "date_naissance": best_match.get('date_naissance', ''),
                "lieu_naissance": best_match.get('lieu_naissance', ''),
                # ── Nationalité ─────────────────────────────────────────────
                "nationalite":    best_match.get('nationalite', []),
                # ── Documents d'identité ────────────────────────────────────
                "passeport":      best_match.get('passeport', []),
                "identification": best_match.get('identification', []),
                # ── Localisation ────────────────────────────────────────────
                "adresse":        best_match.get('adresse', []),
                # ── Sanctions ───────────────────────────────────────────────
                "fondement_jur":  best_match.get('fondement_jur', ''),   # Fondement juridique
                "motifs":         best_match.get('motifs', ''),           # Motifs
                # ── Méta ────────────────────────────────────────────────────
                "source":         self.source or "Inconnue",
                "champs_correspondants": matched_fields,
            }

        if best_score >= SEUIL_BLOCK:
            decision = "BLOCK"
            reason = f"Match GDA fort — Score de similarité : {best_score}/100"
            alert = "GDA"
        elif best_score >= SEUIL_REVIEW:
            decision = "REVIEW"
            reason = f"Match GDA possible — Score de similarité : {best_score}/100"
            alert = "GDA"

        # PPE
        if is_ppe:
            if alert == "GDA":
                alert = "BOTH"
                reason += f" + PPE détecté (score PPE : {ppe_score})"
            else:
                alert = "PPE"
                decision = "REVIEW"
                reason = f"PPE détecté — Score risque : {ppe_score} — Fonctions : {', '.join(ppe_kws)}"

        if ppe_answers and any(ppe_answers.values()):
            if decision == "OK":
                decision = "REVIEW"
                alert = "PPE"

        # Nationalité : NE BLOQUE PAS — alerte informative uniquement
        nationality_risk = get_nationality_risk(client_data.get("nationality", ""))

        return {
            "decision": decision,
            "score": best_score,
            "gda_score": best_score,
            "gda_match": best_score >= SEUIL_REVIEW,
            "gda_details": gda_details,
            "decision_reason": reason,
            "alert_type": alert,
            "match_name": match_name,
            "matched_fields": matched_fields,
            "ppe_keywords": ppe_kws,
            "ppe_score": ppe_score,
            "nationality_risk": nationality_risk,   # Alerte non-bloquante
            "can_subscribe": decision == "OK"
        }


if __name__ == "__main__":
    engine = ScreeningEngine()

    if engine.is_ready():
        print(f"✅ Prêt - {len(engine.entries)} entrées")
