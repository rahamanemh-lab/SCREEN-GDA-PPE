#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io, json, hashlib, unicodedata, re, random, string
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Set, Tuple

import pandas as pd
from rapidfuzz import fuzz, process
import matplotlib.pyplot as plt
import streamlit as st
import requests
from requests.adapters import HTTPAdapter, Retry

# ------------------ Config par défaut ------------------
DEFAULT_TEMPLATE_PATH = Path("test.json")

DEFAULT_TEMPLATE = {
    "first_name": "",
    "last_name": "",
    "birth_date": "",
    "nationality": "",
    "profession": "",
    "email": "",
    "phone_number": "",
    "birth_city": "",
    "birth_country": "",
    "birth_department": ""
}



DEFAULT_REGISTER_PATH = Path("Registrenationaldesgels.json")

N_TOTAL_DEFAULT = 20
SEUIL_BLOCK_DEFAULT = 90
SEUIL_REVIEW_DEFAULT = 80

# ------------------ Base de données PPE (Arrêté du 17 mars 2023) ------------------
PPE_KEYWORDS = {
    # Fonctions politiques de haut niveau
    "président": ["président", "president", "presidencia"],
    "ministre": ["ministre", "minister", "ministro", "ministerio"],
    "premier_ministre": ["premier ministre", "prime minister", "primer ministro"],
    "gouvernement": ["gouvernement", "government", "gobierno", "ministere", "ministerio"],
    "secrétaire_état": ["secrétaire d'état", "secretary of state", "secretario de estado"],
    
    # Fonctions parlementaires
    "député": ["député", "depute", "deputy", "diputado", "member of parliament", "mp"],
    "sénateur": ["sénateur", "senateur", "senator", "senador"],
    "assemblée": ["assemblée nationale", "assembly", "asamblea", "parliament", "parlement"],
    "sénat": ["sénat", "senate", "senado"],
    
    # Fonctions juridictionnelles
    "conseil_état": ["conseil d'état", "conseil d etat", "state council"],
    "conseil_constitutionnel": ["conseil constitutionnel", "constitutional council"],
    "cour_cassation": ["cour de cassation", "supreme court", "cour supreme"],
    "cour_comptes": ["cour des comptes", "court of auditors"],
    "magistrat": ["magistrat", "judge", "juge", "juez"],
    "procureur": ["procureur", "prosecutor", "fiscal"],
    
    # Fonctions diplomatiques
    "ambassadeur": ["ambassadeur", "ambassador", "embajador"],
    "chargé_affaires": ["chargé d'affaires", "charge d affaires", "chargé des affaires"],
    "consul": ["consul", "consulat", "consulate"],
    
    # Fonctions militaires
    "général": ["général", "general", "chef d'état-major", "chief of staff"],
    "amiral": ["amiral", "admiral", "almirante"],
    "colonel": ["colonel", "coronel"],
    "commandant": ["commandant", "commander", "comandante"],
    "état_major": ["état-major", "etat-major", "staff", "estado mayor"],
    
    # Fonctions dans les entreprises publiques
    "directeur_général": ["directeur général", "directeur general", "dg", "ceo", "chief executive"],
    "directeur": ["directeur", "director", "director general"],
    "président_conseil": ["président du conseil", "chairman", "presidente del consejo"],
    "administrateur": ["administrateur", "administrator", "administrador", "board member"],
    "conseil_administration": ["conseil d'administration", "board of directors", "consejo de administracion"],
    "conseil_surveillance": ["conseil de surveillance", "supervisory board"],
    
    # Fonctions dans les partis politiques
    "secrétaire_général": ["secrétaire général", "secretary general", "secretario general"],
    "président_parti": ["président de parti", "party leader", "presidente del partido"],
    "bureau_politique": ["bureau politique", "political bureau"],
    
    # Fonctions bancaires/financières
    "banque_centrale": ["banque centrale", "central bank", "banco central"],
    "gouverneur": ["gouverneur", "governor", "gobernador"],
    "banque_france": ["banque de france", "bank of france"],
    
    # Fonctions locales importantes
    "maire": ["maire", "mayor", "alcalde"],
    "préfet": ["préfet", "prefect", "prefecto"],
    "président_région": ["président de région", "regional president"],
    "président_département": ["président de département", "departmental president"],
    
    # Organisations internationales
    "onu": ["onu", "un", "united nations", "naciones unidas"],
    "union_européenne": ["union européenne", "european union", "ue", "eu"],
    "commission_européenne": ["commission européenne", "european commission"],
    "parlement_européen": ["parlement européen", "european parliament"],
    "otan": ["otan", "nato"],
    "fmi": ["fmi", "imf", "international monetary fund"],
    "banque_mondiale": ["banque mondiale", "world bank"],
}

# Mots-clés spécifiques pour identifier les entreprises publiques
PUBLIC_COMPANY_KEYWORDS = [
    "établissement public", "entreprise publique", "société d'économie mixte", "sem",
    "société publique locale", "spl", "epic", "epa", "gie public",
    "régie", "syndicat mixte", "public company", "state enterprise"
]

def detect_ppe_risk(client_data: Dict[str, Any]) -> Tuple[bool, int, List[str], str]:
    """
    Détecte si un client présente un risque PPE basé sur :
    - Nom/prénom contenant des indices de fonction politique
    - Profession mentionnée
    - Lieu de travail
    - Informations contextuelles
    
    Retourne: (is_ppe_risk, risk_score, matched_keywords, risk_explanation)
    """
    risk_score = 0
    matched_keywords = []
    risk_factors = []
    
    # Collecte toutes les informations textuelles du client
    text_fields = []
    if client_data.get("first_name"):
        text_fields.append(client_data["first_name"])
    if client_data.get("last_name"):
        text_fields.append(client_data["last_name"])
    if client_data.get("profession"):
        text_fields.append(client_data["profession"])
    if client_data.get("employer"):
        text_fields.append(client_data["employer"])
    if client_data.get("job_title"):
        text_fields.append(client_data["job_title"])
    if client_data.get("workplace"):
        text_fields.append(client_data["workplace"])
    if client_data.get("address"):
        text_fields.append(client_data["address"])
    
    full_text = " ".join(text_fields).lower()
    normalized_text = norm(full_text)

    # Mots qui ne doivent jamais déclencher un signal PPE
    EXCLUDED_TERMS = {"commercial", "commerce", "communication"}  # pas de signal PPE si seuls ces mots matchent
    SHORT_TOKENS = {"ue", "eu", "un"}  # tokens trop courts → match strict sur mot entier en majuscules dans le texte brut


# Vérification des mots-clés PPE (robuste)
    for category, keywords in PPE_KEYWORDS.items():
        for keyword in keywords:
            kw_norm = norm(keyword)
            if kw_norm in EXCLUDED_TERMS:
                continue

            # Cas des tokens très courts (UE/UN/EU) → on exige un mot entier en MAJ dans le texte d'origine
            if kw_norm in SHORT_TOKENS:
                if not re.search(r"\b(UE|UN|EU)\b", full_text):
                    continue
            else:
                # match normal sécurisé : mot entier dans le texte normalisé
                if not re.search(rf"\b{re.escape(kw_norm)}\b", normalized_text):
                    continue

            # Score plus élevé pour les fonctions de haut niveau
            if category in ["président", "ministre", "premier_ministre"]:
                score_boost = 25
            elif category in ["député", "sénateur", "ambassadeur", "général"]:
                score_boost = 20
            elif category in ["maire", "préfet", "directeur_général"]:
                score_boost = 15
            else:
                score_boost = 10

            risk_score += score_boost
            matched_keywords.append(f"{category}:{keyword}")
            risk_factors.append(f"Fonction potentielle: {keyword}")

    
    # Vérification entreprises publiques
    for keyword in PUBLIC_COMPANY_KEYWORDS:
        if norm(keyword) in normalized_text:
            risk_score += 8
            matched_keywords.append(f"public_company:{keyword}")
            risk_factors.append(f"Entreprise publique: {keyword}")
    
    # Vérification des titres honorifiques et indices
    honorary_titles = ["son excellence", "honorable", "dr.", "prof.", "professeur"]
    for title in honorary_titles:
        if norm(title) in normalized_text:
            risk_score += 5
            matched_keywords.append(f"title:{title}")
            risk_factors.append(f"Titre: {title}")
    
    # Seuils de détection PPE
    is_ppe_risk = risk_score >= 15  # Seuil ajustable
    
    risk_explanation = "; ".join(risk_factors) if risk_factors else "Aucun indicateur PPE détecté"
    
    return is_ppe_risk, risk_score, matched_keywords, risk_explanation

# ------------------ Helpers existants ------------------
def norm(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    return " ".join(s.split())

def random_date_ddmmyyyy(start_year=1950, end_year=2003) -> str:
    start = date(start_year,1,1); end = date(end_year,12,31)
    d = start + timedelta(days=random.randint(0, (end-start).days))
    return d.strftime("%d/%m/%Y")

def to_iso_birth(dmy: str) -> str:
    try: return datetime.strptime(dmy, "%d/%m/%Y").strftime("%Y-%m-%d")
    except Exception: return ""

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""): h.update(chunk)
    return h.hexdigest()

def clone_with(template: Dict[str,Any], **overrides) -> Dict[str,Any]:
    d = dict(template); d.update(overrides); return d

def rand_suffix(n=6) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return ''.join(random.choice(alphabet) for _ in range(n))

def make_email(first, last) -> str:
    base = f"{norm(first) or 'user'}.{norm(last) or 'x'}"
    return f"{base}.{rand_suffix()}@example.com"

def make_phone() -> str:
    return "+336" + "".join(random.choice("0123456789") for _ in range(8))


API_BASE = "https://gels-avoirs.dgtresor.gouv.fr/ApiPublic/api/v1/publication"

def _requests_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

@st.cache_data(ttl=3600)  # cache 1h
def fetch_register_latest_json() -> Dict[str, Any]:
    """
    Récupère le flux JSON de la dernière publication (API publique DGT).
    Voir Swagger officiel (format d'appel évolué le 21/01/2025).
    """
    url = f"{API_BASE}/derniere-publication-flux-json"
    sess = _requests_session()
    r = sess.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=3600)
def fetch_register_latest_date() -> str:
    """Optionnel : récupérer la date de dernière publication (si exposée)."""
    url = f"{API_BASE}/derniere-publication-date"
    sess = _requests_session()
    r = sess.get(url, timeout=15)
    if r.ok:
        try:
            return r.json().get("datePublication") or r.text.strip()
        except Exception:
            return r.text.strip()
    return ""







# ------------------ Registre parsing ------------------
def parse_register(reg_json: Dict[str,Any]) -> Dict[str,Any]:
    """
    Extrait :
      - display_names (Nom/Prénom, ALIAS, AUTRE_IDENTITE)
      - dob (YYYY-MM-DD ou partiel genre 'YYYY' / 'YYYY-MM')
      - nationalities (liste)
      - birth_place (optionnel si présent)
      - nature, ref_ue, ref_onu
    """
    pub = reg_json.get("Publications", {})
    date_pub = pub.get("DatePublication")
    items = pub.get("PublicationDetail", [])
    entries = []
    for it in items:
        nom = it.get("Nom","")
        nature = it.get("Nature","")
        ref_ue, ref_onu = [], []
        prenoms, aliases, autres_identites, nationalities = [], [], [], []
        dob = None
        birth_place = None

        for d in it.get("RegistreDetail", []):
            t = d.get("TypeChamp"); val = d.get("Valeur") or []
            if t == "PRENOM":
                prenoms += [x.get("Prenom") for x in val if x.get("Prenom")]
            elif t == "ALIAS":
                aliases += [x.get("Alias") for x in val if x.get("Alias")]
            elif t == "AUTRE_IDENTITE":
                autres_identites += [x.get("AutreIdentite") for x in val if x.get("AutreIdentite")]
            elif t == "NATIONALITE":
                nationalities += [x.get("Pays") for x in val if x.get("Pays")]
            elif t == "LIEU_DE_NAISSANCE":
                # Certaines entrées peuvent fournir ville/pays; on concatène simplement
                # Exemple d'objet attendu : {"LieuNaissance": "Casablanca, Maroc"}
                for x in val:
                    bp = x.get("LieuNaissance") or x.get("Lieu") or x.get("Ville")
                    if bp:
                        birth_place = bp if birth_place is None else f"{birth_place}; {bp}"
            elif t == "DATE_DE_NAISSANCE":
                # Le registre peut donner Annee, Mois, Jour partiels
                x = val[0] if val else {}
                y = x.get("Annee")
                m = x.get("Mois")
                j = x.get("Jour")
                if y:
                    if m and j:
                        dob = f"{y}-{str(m).zfill(2)}-{str(j).zfill(2)}"
                    elif m:
                        dob = f"{y}-{str(m).zfill(2)}"     # YYYY-MM
                    else:
                        dob = f"{y}"                        # YYYY
            elif t == "REFERENCE_UE":
                ref_ue += [x.get("ReferenceUe") for x in val if x.get("ReferenceUe")]
            elif t == "REFERENCE_ONU":
                ref_onu += [x.get("ReferenceOnu") for x in val if x.get("ReferenceOnu")]

        display_names = []
        if nature == "Personne physique" and prenoms:
            for p in prenoms:
                display_names.append(f"{p} {nom}")
        else:
            display_names.append(nom)

        # Ajoute alias + autres identités
        display_names += aliases
        display_names += autres_identites

        for dn in [n for n in display_names if n]:
            entries.append({
                "display_name": dn,
                "norm": norm(dn),
                "dob": dob,  # peut être YYYY, YYYY-MM ou YYYY-MM-DD
                "nationalities": nationalities,
                "birth_place": birth_place,
                "nature": nature,
                "ref_ue": ref_ue,
                "ref_onu": ref_onu
            })
    return {"date_publication": date_pub, "entries": entries}

# ------------------ Génération POS/NEG avec possibilité PPE ------------------
FIRST = ["Jean","Marie","Ahmed","Fatima","John","Emily","Carlos","Maria","Wei","Li","Aisha","Omar",
         "Emmanuel", "François", "Nicolas", "Marine", "Jean-Luc", "Bruno", "Édouard"]
LAST  = ["Dupont","Martin","Hassan","Garcia","Smith","Zhang","Khan","Ibrahim","Lopez","Silva","Nguyen","Rossi",
         "Macron", "Hollande", "Sarkozy", "Le Pen", "Mélenchon", "Maire", "Philippe"]
NATS  = ["Française","Espagnole","Marocaine","Égyptienne","Italienne","Chinoise","Indienne","Saoudienne","Américaine","Russe"]

# Professions potentiellement PPE pour la génération
PPE_PROFESSIONS = [
    "Ministre", "Député", "Sénateur", "Maire", "Préfet", "Ambassadeur",
    "Directeur Général", "Président", "Secrétaire Général", "Gouverneur",
    "Magistrat", "Procureur", "Général", "Amiral", "Colonel",
    "Directeur d'établissement public", "Administrateur"
]

REGULAR_PROFESSIONS = [
    "Ingénieur", "Médecin", "Avocat", "Professeur", "Consultant", "Commercial",
    "Comptable", "Architecte", "Journaliste", "Entrepreneur", "Cadre"
]


# Pools simples (tu peux enrichir)
BIRTH_COUNTRIES = ["France","Maroc","Algérie","Tunisie","Égypte","Italie","Espagne","Allemagne","Turquie","Birmanie/Myanmar","Iran","Afghanistan","Yémen"]
BIRTH_CITIES = {
    "France": ["Paris","Lyon","Marseille","Lille","Bordeaux","Toulouse","Nice","Nantes","Rennes","Strasbourg"],
    "Maroc": ["Casablanca","Rabat","Fès","Marrakech"],
    "Algérie": ["Alger","Oran","Constantine"],
    "Égypte": ["Le Caire","Alexandrie"],
    "Italie": ["Rome","Milan","Naples"],
    "Espagne": ["Madrid","Barcelone","Valence"],
    "Allemagne": ["Berlin","Hambourg","Munich"],
    "Turquie": ["Istanbul","Ankara","Izmir"],
    "Birmanie/Myanmar": ["Yangon","Mandalay","Naypyidaw"],
    "Iran": ["Téhéran","Ispahan","Machhad"],
    "Afghanistan": ["Kaboul","Kandahar","Herat"],
    "Yémen": ["Sanaa","Aden","Hodeïda"]
}
FR_DEPARTMENTS = ["75","69","13","59","33","31","06","44","35","67","93","94","92","91","95","77","78"]

def sample_birthplace(nationality:str):
    # pays
    if nationality and "fran" in norm(nationality):
        country = "France"
    else:
        country = random.choice(BIRTH_COUNTRIES)
    # ville
    city = random.choice(BIRTH_CITIES.get(country, ["—"]))
    # département si France
    dep = random.choice(FR_DEPARTMENTS) if country == "France" else ""
    return city, country, dep








def make_pos_record(template: Dict[str,Any], e: Dict[str,Any]) -> Dict[str,Any]:
    parts = e["display_name"].split()
    first = parts[0] if parts else "X"
    last  = " ".join(parts[1:]) if len(parts) > 1 else ""
    if e["dob"] and len(e["dob"]) == 10 and e["dob"].count("-") == 2:
        try:
            birth = datetime.strptime(e["dob"], "%Y-%m-%d").strftime("%d/%m/%Y")
        except Exception:
            birth = random_date_ddmmyyyy()
    else:
        birth = random_date_ddmmyyyy()
    nat = (e["nationalities"][0] if e["nationalities"] else random.choice(NATS))
    profession = random.choice(PPE_PROFESSIONS) if random.random() < 0.3 else random.choice(REGULAR_PROFESSIONS)

    # >>> NEW: birthplace généré systématiquement
    city, country, dep = sample_birthplace(nat)

    return clone_with(
        template,
        first_name=first,
        last_name=last,
        birth_date=birth,
        nationality=nat,
        email=make_email(first,last),
        phone_number=make_phone(),
        profession=profession,
        birth_city=city,               # on écrase le template
        birth_country=country,
        birth_department=dep,
    )


def make_neg_record(template: Dict[str,Any]) -> Dict[str,Any]:
    first, last = random.choice(FIRST), random.choice(LAST)
    nat = random.choice(NATS)
    profession = random.choice(PPE_PROFESSIONS) if random.random() < 0.1 else random.choice(REGULAR_PROFESSIONS)
    city, country, dep = sample_birthplace(nat)

    return clone_with(
        template,
        first_name=first,
        last_name=last,
        birth_date=random_date_ddmmyyyy(),
        nationality=nat,
        email=make_email(first,last),
        phone_number=make_phone(),
        profession=profession,
        birth_city=city,               # on écrase le template
        birth_country=country,
        birth_department=dep,
    )


# ------------------ Screening avec explicabilité étendue ------------------
def screen_records(records: List[Dict[str,Any]], entries: List[Dict[str,Any]], seuil_block: int, seuil_review: int) -> pd.DataFrame:
    """
    Calcule:
      - score_name (RapidFuzz token_sort_ratio)
      - bonus_dob_exact (+8 si YYYY-MM-DD exact)
      - bonus_dob_year (+3 si même année; +5 si même année+mois)
      - bonus_nationality (+4 si préfixe de nationalité concordant)
      - bonus_birthplace (+3 si lieu client est inclus dans lieu registre)
      - ppe_risk_score (score de risque PPE)
      - bonus_ppe (+10 si PPE détecté)
      -> score total = score_name + bonus* + bonus_ppe
      Décision: OK / REVIEW / BLOCK/REVIEW
      Explicabilité: matched_fields, decision_reason, ppe_indicators
    """
    norm_names = [e["norm"] for e in entries]
    rows = []
    for i, rec in enumerate(records, start=1):
        full = f"{rec.get('first_name','').strip()} {rec.get('last_name','').strip()}".strip()
        nm = norm(full)
        matches = process.extract(nm, norm_names, scorer=fuzz.token_sort_ratio, score_cutoff=30, limit=5)

        # Détection PPE
        is_ppe_risk, ppe_risk_score, ppe_keywords, ppe_explanation = detect_ppe_risk(rec)

        best_total = 0
        best_entry = None
        best_break = {
            "score_name": 0, "bonus_dob_exact": 0, "bonus_dob_year": 0, 
            "bonus_nationality": 0, "bonus_birthplace": 0, "bonus_ppe": 0,
            "matched_fields": "", "ppe_risk_score": ppe_risk_score,
            "is_ppe_risk": is_ppe_risk, "ppe_keywords": ppe_keywords,
            "ppe_explanation": ppe_explanation
        }

        dob_iso = to_iso_birth(rec.get("birth_date",""))
        nat_client = rec.get("nationality","")
        birth_place_client = rec.get("birth_place","")

        for m in matches:
            matched_norm, score_name = m[0], m[1]
            for e in entries:
                if e["norm"] != matched_norm:
                    continue

                total = score_name
                bonus_dob_exact = 0
                bonus_dob_year  = 0
                bonus_nat       = 0
                bonus_place     = 0
                bonus_ppe       = 0

                # DOB exacte (YYYY-MM-DD)
                if dob_iso and e.get("dob") and len(e["dob"]) == 10 and e["dob"].count("-") == 2:
                    if dob_iso == e["dob"]:
                        bonus_dob_exact = 8
                # DOB partielle (YYYY ou YYYY-MM côté registre)
                elif dob_iso and e.get("dob"):
                    y_c = dob_iso[:4]; y_e = e["dob"][:4]
                    if y_c and y_e and y_c == y_e:
                        bonus_dob_year = 3
                        # si année+mois concordent (registre au format YYYY-MM)
                        if len(e["dob"]) >= 7 and e["dob"][:7] == dob_iso[:7]:
                            bonus_dob_year = 5

                # Nationalité (préfixe 3 lettres)
                if nat_client and e.get("nationalities"):
                    if any(norm(nat_client).startswith(norm(n)[:3]) for n in e["nationalities"]):
                        bonus_nat = 4

                # Lieu de naissance (si dispo côté client + côté registre)
                if birth_place_client and e.get("birth_place"):
                    bp_c = norm(birth_place_client)
                    bp_e = norm(e["birth_place"])
                    if bp_c and bp_e and (bp_c in bp_e or bp_e in bp_c):
                        bonus_place = 3

                # Bonus PPE
                if is_ppe_risk:
                    bonus_ppe = min(10, ppe_risk_score // 2)  # Max 10 points pour PPE

                total += bonus_dob_exact + bonus_dob_year + bonus_nat + bonus_place + bonus_ppe

                if total > best_total:
                    best_total = total
                    best_entry = e
                    matched_fields = ["name"]
                    if bonus_dob_exact: matched_fields.append("dob_exact")
                    elif bonus_dob_year >= 5: matched_fields.append("dob_year_month")
                    elif bonus_dob_year >= 3: matched_fields.append("dob_year")
                    if bonus_nat: matched_fields.append("nationality")
                    if bonus_place: matched_fields.append("birth_place")
                    if bonus_ppe: matched_fields.append("ppe_risk")
                    
                    best_break = dict(
                        score_name=score_name,
                        bonus_dob_exact=bonus_dob_exact,
                        bonus_dob_year=bonus_dob_year,
                        bonus_nationality=bonus_nat,
                        bonus_birthplace=bonus_place,
                        bonus_ppe=bonus_ppe,
                        matched_fields=",".join(matched_fields),
                        ppe_risk_score=ppe_risk_score,
                        is_ppe_risk=is_ppe_risk,
                        ppe_keywords=ppe_keywords,
                        ppe_explanation=ppe_explanation
                    )

        # Si pas de match registre mais PPE détecté, on garde le score PPE
        if best_total == 0 and is_ppe_risk:
            best_total = ppe_risk_score
            best_break["matched_fields"] = "ppe_only"

        decision = "OK"
        if best_total >= seuil_block:
            decision = "BLOCK/REVIEW"
        elif best_total >= seuil_review:
            decision = "REVIEW"

        # ---------------- Prudence ADAPTATIVE ----------------
        # Un "signal fort" = au moins un de ces critères :
        has_strong_signal = (
                best_break.get("score_name", 0) >= 70 or
                best_break.get("bonus_dob_exact", 0) > 0 or
                best_break.get("bonus_dob_year", 0) >= 5 or
                (best_break.get("bonus_nationality", 0) > 0 and best_break.get("bonus_birthplace", 0) > 0)
        )

        # PPE seul mais faible → ne force pas
        ppe_is_weak = (is_ppe_risk and best_break.get("ppe_risk_score", 0) < 20 and best_break.get("score_name", 0) < 60)

        # Appliquer prudence seulement si activée ET signal fort
        if strict_prudence and decision == "OK":
            if (best_entry and has_strong_signal) or (is_ppe_risk and not ppe_is_weak and has_strong_signal):
                decision = "REVIEW"
                forced_prudence = True
            else:
                forced_prudence = False
        else:
            forced_prudence = False
        # ------------------------------------------------------

        # Raison lisible étendue
        reasons = []
        if best_break.get("score_name",0) >= 90: reasons.append("nom quasi identique")
        elif best_break.get("score_name",0) >= 80: reasons.append("nom très proche")
        elif best_break.get("score_name",0) >= 70: reasons.append("nom proche")
        if best_break.get("bonus_dob_exact",0): reasons.append("DOB exacte")
        elif best_break.get("bonus_dob_year",0) >= 5: reasons.append("DOB année+mois OK")
        elif best_break.get("bonus_dob_year",0) >= 3: reasons.append("DOB année OK")
        if best_break.get("bonus_nationality",0): reasons.append("nationalité concordante")
        if best_break.get("bonus_birthplace",0): reasons.append("lieu de naissance concordant")
        if best_break.get("bonus_ppe",0): reasons.append(f"PPE détecté ({ppe_explanation})")
        elif is_ppe_risk: reasons.append(f"PPE potentiel ({ppe_explanation})")
        if forced_prudence:
            reasons.append("règle de prudence (GDA/PPE)")

        decision_reason = " + ".join(reasons) if reasons else "match faible"


        # Typologie d'alerte
        alert_type = []
        if best_entry:
            alert_type.append("GDA")  # Gel Des Avoirs
        if is_ppe_risk:
            alert_type.append("PPE")
        alert_type_str = " + ".join(alert_type) if alert_type else "Aucune"

        rows.append({
            "client_id": f"C{i:03d}",
            "client_full_name": full,
            "birth_date": rec.get("birth_date",""),
            "nationality": nat_client,
            "profession": rec.get("profession", ""),
            "score": best_total,
            "decision": decision,
            "decision_reason": decision_reason,
            "alert_type": alert_type_str,
            "score_name": best_break.get("score_name",0),
            "bonus_dob_exact": best_break.get("bonus_dob_exact",0),
            "bonus_dob_year": best_break.get("bonus_dob_year",0),
            "bonus_nationality": best_break.get("bonus_nationality",0),
            "bonus_birthplace": best_break.get("bonus_birthplace",0),
            "bonus_ppe": best_break.get("bonus_ppe",0),
            "ppe_risk_score": ppe_risk_score,
            "is_ppe_risk": is_ppe_risk,
            "ppe_explanation": ppe_explanation,
            "ppe_keywords": ";".join(ppe_keywords),
            "matched_fields": best_break.get("matched_fields",""),
            "match_name": (best_entry["display_name"] if best_entry else ""),
            "nature": (best_entry["nature"] if best_entry else ""),
            "entry_dob": (best_entry.get("dob","") if best_entry else ""),
            "entry_nationalities": ";".join(best_entry.get("nationalities") or []) if best_entry else "",
            "entry_birth_place": (best_entry.get("birth_place","") if best_entry else ""),
            "ref_ue": ";".join(best_entry.get("ref_ue") or []) if best_entry else "",
            "ref_onu": ";".join(best_entry.get("ref_onu") or []) if best_entry else ""
        })
    return pd.DataFrame(rows)

# ------------------ Streamlit UI ------------------
st.set_page_config(page_title="Screening Gel des Avoirs + PPE — Suite POC", layout="wide")
st.title("🔍 Screening Gel des Avoirs + PPE — Suite POC (Registre FR)")
st.caption("Génère des clients (mêmes clés que test.json), screen vs Registre FR + détection PPE, visualise et exporte. Explicabilité incluse (raison du REVIEW/BLOCK + typologie d'alerte GDA/PPE).")

with st.sidebar:
    st.header("⚙️ Paramètres")
    review_th = st.slider("Seuil REVIEW (≥)", 60, 95, SEUIL_REVIEW_DEFAULT, 1)
    block_th  = st.slider("Seuil BLOCK/REVIEW (≥)", 70, 100, SEUIL_BLOCK_DEFAULT, 1)
    n_total   = st.slider("Nb. clients à générer", 10, 100, N_TOTAL_DEFAULT, 10)

    st.markdown("---")
    st.subheader("📄 Fichiers d'entrée")
    tmpl_file = st.file_uploader("test.json (gabarit)", type=["json"])
    # reg_file  = st.file_uploader("Registrenationaldesgels.json", type=["json"])

    use_local_files = st.checkbox("Utiliser les fichiers locaux si non uploadés", value=True)
    st.markdown("---")
    st.subheader("📚 Source du Registre")
    reg_source = st.radio("Sélection de la source", ["API (recommandé)", "Fichier JSON local"], index=0)


    st.markdown("---")
    strict_prudence = st.checkbox("Prudence stricte GDA/PPE (forcer REVIEW)", value=True)


# Charger le registre via API ou fichier
reg_json = None
reg_sha = ""
reg_date_pub = ""


# Charger template
if tmpl_file is not None:
    template = json.load(tmpl_file)
elif use_local_files and DEFAULT_TEMPLATE_PATH.exists():
    template = json.loads(DEFAULT_TEMPLATE_PATH.read_text(encoding="utf-8"))
else:
    st.error("Charge un test.json ou coche l'option 'Utiliser les fichiers locaux'.")
    st.stop()

# Charger registre
reg_json = None
reg_sha = ""
reg_date_pub = ""


try:
    if reg_source == "API (recommandé)":
        reg_json = fetch_register_latest_json()
        # si la date est imbriquée comme avant
        parsed_try = reg_json.get("Publications", {})
        reg_date_pub = parsed_try.get("DatePublication") or fetch_register_latest_date()
        # empreinte pour audit (hash du texte brut)
        reg_sha = sha256_bytes(json.dumps(reg_json, ensure_ascii=False).encode("utf-8"))
    else:
        reg_file  = st.file_uploader("Registrenationaldesgels.json (option locale)", type=["json"])
        if reg_file is not None:
            data = reg_file.read()
            reg_sha = sha256_bytes(data)
            reg_json = json.loads(data.decode("utf-8"))
        elif DEFAULT_REGISTER_PATH.exists():
            reg_json = json.loads(DEFAULT_REGISTER_PATH.read_text(encoding="utf-8"))
            reg_sha = sha256_file(DEFAULT_REGISTER_PATH)
        else:
            st.error("Charge un JSON local OU choisis la source API.")
            st.stop()
except requests.RequestException as e:
    st.error(f"Erreur d'accès à l'API du Registre : {e}")
    st.stop()

parsed = parse_register(reg_json)
entries = parsed["entries"]
if not reg_date_pub:
    reg_date_pub = parsed.get("date_publication","")

# ------------------ Génération / Import / UI en onglets ------------------
tab_gen, tab_res, tab_exp = st.tabs(["🧪 Générer / Importer", "📊 Résultats & Visuels", "⬇️ Exports"])

with tab_res:
    # st.subheader("Screening")
    if reg_date_pub:
        st.caption(f"📅 Données du registre publiées le : **{reg_date_pub}** (source {'API' if reg_source.startswith('API') else 'fichier local'})")

with tab_gen:
    st.subheader("Source des clients")
    mode = st.radio("Choisis une source", ["Générer (moitié POS/NEG)", "Importer CSV", "Importer JSONL"])

    # Résolution du template SEULEMENT maintenant
    template = None
    if tmpl_file is not None:
        template = json.load(tmpl_file)
    elif use_local_files and DEFAULT_TEMPLATE_PATH.exists():
        template = json.loads(DEFAULT_TEMPLATE_PATH.read_text(encoding="utf-8"))
    else:
        # aucun fichier fourni → on utilise un gabarit en mémoire
        template = DEFAULT_TEMPLATE

    if mode == "Générer (moitié POS/NEG)":
        if st.button("Générer maintenant"):
            # tu peux utiliser 'template' sans uploader quoi que ce soit
            personnes = [e for e in entries if e["nature"] == "Personne physique" and e["display_name"]]
            pool = personnes or entries
            gen = []
            N_POS = n_total // 2
            N_NEG = n_total - N_POS
            for _ in range(N_POS): gen.append(make_pos_record(template, random.choice(pool)))
            for _ in range(N_NEG): gen.append(make_neg_record(template))
            for i, r in enumerate(gen, start=1): r["_client_id"] = f"C{i:03d}"
            st.session_state["records"] = gen
            st.success(f"{len(gen)} dossiers générés.")

    elif mode == "Importer CSV":
        csv_up = st.file_uploader("Importer un CSV (mêmes clés que test.json)", type=["csv"])
        if csv_up is not None:
            df_in = pd.read_csv(csv_up, dtype=str).fillna("")
            st.session_state["records"] = df_in.to_dict(orient="records")
            st.success(f"{len(st.session_state['records'])} dossiers importés.")

    else:  # Importer JSONL
        jsonl_up = st.file_uploader("Importer un JSONL", type=["jsonl","json","txt"])
        if jsonl_up is not None:
            recs = [json.loads(line) for line in jsonl_up.read().decode("utf-8").splitlines() if line.strip()]
            st.session_state["records"] = recs
            st.success(f"{len(recs)} dossiers importés.")

    # --- après avoir mis st.session_state["records"] ---
    if "records" in st.session_state and st.session_state["records"]:
        raw_df = pd.DataFrame(st.session_state["records"])
        st.markdown("### 📄 Table complète (toutes colonnes d'origine)")
        st.dataframe(raw_df, use_container_width=True)

        # Colonnes importantes pour le screening (ne garder que celles qui existent)
        IMPORTANT_FIELDS = [
            "first_name", "last_name", "birth_date", "nationality",
            "profession", "job_title", "employer", "workplace", "address",
            "birth_city", "birth_country", "birth_department"
        ]
        keep_cols = [c for c in IMPORTANT_FIELDS if c in raw_df.columns]

        st.markdown("### 🧹 Table dépouillée (champs utiles au screening)")
        if keep_cols:
            stripped_df = raw_df[keep_cols].copy()
        else:
            # fallback si aucune des colonnes attendues n'existe
            stripped_df = raw_df.copy()  # au moins afficher quelque chose

        st.dataframe(stripped_df, use_container_width=True)
    else:
        st.warning("Aucun dossier chargé pour l’instant.")


with tab_res:
    st.subheader("Screening")
    if "records" not in st.session_state or not st.session_state["records"]:
        st.info("Charge ou génère d’abord des dossiers dans l’onglet précédent.")
    else:
        if st.button("Lancer le screening"):
            df = screen_records(st.session_state["records"], entries, block_th, review_th)
            st.session_state["results_df"] = df
            st.success("Screening terminé.")

    if "results_df" in st.session_state:
        df = st.session_state["results_df"]
        # KPIs
        c1, c2, c3, c4 = st.columns(4)
        total = len(df)
        n_ok = (df["decision"] == "OK").sum()
        n_rev = (df["decision"] == "REVIEW").sum()
        n_blk = (df["decision"] == "BLOCK/REVIEW").sum()
        c1.metric("Dossiers", total)
        c2.metric("OK", n_ok)
        c3.metric("À revoir", n_rev)
        c4.metric("Blocage/Review", n_blk)

        st.markdown("---")
        # Pie chart (décisions)
        counts = df["decision"].value_counts()
        fig1, ax1 = plt.subplots()
        ax1.pie(counts, labels=counts.index, autopct="%1.0f%%", startangle=90)
        ax1.axis("equal")
        st.pyplot(fig1)

        # Bar chart top N (scores)
        st.subheader("Top scores de matching")
        top_n = st.slider("Top N", 5, 50, 10, 1)
        top = df.sort_values("score", ascending=False).head(top_n)
        fig2, ax2 = plt.subplots(figsize=(max(6, top_n*0.6), 4))
        ax2.bar(top["client_full_name"], top["score"])
        ax2.set_ylabel("Score")
        ax2.set_xticklabels(top["client_full_name"], rotation=45, ha="right")
        st.pyplot(fig2)

        # Table filtrable + recherche PPE
        st.subheader("Table des résultats")
        display_cols = [c for c in [
            "client_id","client_full_name","score","decision","decision_reason",
            "score_name","bonus_dob_exact","bonus_dob_year","bonus_nationality","bonus_birthplace","bonus_ppe","ppe_risk_score","is_ppe_risk","ppe_keywords",
            "matched_fields","birth_date","entry_dob","nationality","entry_nationalities","entry_birth_place",
            "match_name","nature","ref_ue","ref_onu","profession","alert_type"
        ] if c in df.columns]

        filt = st.multiselect("Filtrer par décision", df["decision"].unique().tolist(), default=[])
        show_ppe_only = st.checkbox("Afficher uniquement cas avec indicateur PPE", value=False)
        shown = df if not filt else df[df["decision"].isin(filt)]
        if show_ppe_only:
            shown = shown[shown["is_ppe_risk"] == True]
        st.dataframe(shown[display_cols] if display_cols else shown, use_container_width=True)

with tab_exp:
    if "results_df" not in st.session_state:
        st.info("Réalise d’abord un screening pour activer les exports.")
    else:
        df = st.session_state["results_df"]

        # Export résultats
        st.subheader("Exports")
        res_csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Télécharger résultats (CSV)", data=res_csv, file_name="screening_results.csv", mime="text/csv")

        review_cases = df[df["decision"]!="OK"].copy()
        review_csv = review_cases.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Télécharger cas à revoir (CSV)", data=review_csv, file_name="review_cases.csv", mime="text/csv")

        # Export Excel multi-onglets (optionnel)
        if st.button("Générer Excel multi-onglets"):
            from io import BytesIO
            out = BytesIO()
            with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
                df.to_excel(writer, sheet_name="results", index=False)
                review_cases.to_excel(writer, sheet_name="to_review", index=False)
                # Statistiques sommaires
                stats = pd.DataFrame({
                    "metric": ["total","ok","review","block"],
                    "value": [len(df), n_ok, n_rev, n_blk]
                })
                stats.to_excel(writer, sheet_name="stats", index=False)
                writer.save()
            st.download_button("⬇️ Télécharger rapport Excel", data=out.getvalue(), file_name="screening_report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # Audit JSON
        audit = {
            "executed_at": datetime.utcnow().isoformat()+"Z",
            "register_sha256": (reg_sha[:64] if reg_sha else ""),
            "register_date_publication": reg_date_pub,
            "n_register_entries": len(entries),
            "thresholds": {"block": block_th, "review": review_th},
            "inputs": {
                "mode": "generated/imported",
                "n_records": len(st.session_state.get("records", []))
            }
        }
        audit_json = json.dumps(audit, ensure_ascii=False, indent=2).encode("utf-8")
        st.download_button("⬇️ Télécharger audit.json", data=audit_json, file_name="audit.json", mime="application/json")

        st.caption("Note : Le registre FR couvre les gels d'avoirs ; la détection PPE est heuristique (mots-clés). Pour une production, connecter une source PEP dédiée & règles métier supplémentaires.")
