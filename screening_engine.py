#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Moteur de screening GDA/PPE
Source primaire  : API officielle France (DG Trésor)
Source de repli  : API officielle Monaco
Bascule automatique si France indisponible après timeout
"""

import requests
import unicodedata
from typing import Dict, Any, List, Tuple, Optional
from rapidfuzz import fuzz, process

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

SENSITIVE_NATIONALITIES = {
    # Liste officielle DG Trésor - Sanctions économiques
    "russie": ["russe", "russia", "russian", "russie"],
    "bielorussie": ["bielorusse", "belarus", "belarusian", "biélorussie"],
    "coree_nord": ["nord-coréen", "nord-coréenne", "north korean", "coree du nord", "corée du nord", "dprk"],
    "iran": ["iranien", "iranienne", "iran", "iranian"],
    "syrie": ["syrien", "syrienne", "syria", "syrian", "syrie"],
    "myanmar": ["birman", "birmane", "myanmar", "burmese", "birmanie"],
    "venezuela": ["vénézuélien", "vénézuélienne", "venezuelan", "venezuela"],
    "zimbabwe": ["zimbabwéen", "zimbabwéenne", "zimbabwean", "zimbabwe"],
    "nicaragua": ["nicaraguayen", "nicaraguayenne", "nicaraguan", "nicaragua"],
    "soudan": ["soudanais", "soudanaise", "sudanese", "sudan", "soudan"],
    "yemen": ["yéménite", "yemeni", "yemen", "yémen"],
    "libye": ["libyen", "libyenne", "libyan", "libya", "libye"],
    "somalie": ["somalien", "somalienne", "somali", "somalian", "somalie"],
    "rca": ["centrafricain", "centrafricaine", "central african", "république centrafricaine", "rca"],
    "congo_rdc": ["congolais", "congolaise", "congolese", "république démocratique du congo", "rdc", "congo"],
    "mali": ["malien", "malienne", "malian", "mali"],
    "haiti": ["haïtien", "haïtienne", "haitian", "haiti", "haïti"],
    "liban": ["libanais", "libanaise", "lebanese", "lebanon", "liban"],
    # Ajout Chine pour cohérence (souvent considéré comme sensible en AML/CFT)
    "chine": ["chinois", "chinoise", "china", "chinese", "chine"],
}

def norm(text: str) -> str:
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

                prenoms       = []
                nationalities = []
                dob           = ""

                for detail in item.get("RegistreDetail", []):
                    type_champ = detail.get("TypeChamp")
                    valeurs    = detail.get("Valeur") or []

                    if type_champ == "PRENOM":
                        prenoms += [v.get("Prenom", "") for v in valeurs if v.get("Prenom")]

                    elif type_champ == "NATIONALITE":
                        nationalities += [v.get("Pays", "") for v in valeurs if v.get("Pays")]

                    elif type_champ == "DATE_DE_NAISSANCE":
                        v = valeurs[0] if valeurs else {}
                        y = v.get("Annee", "")
                        m = v.get("Mois", "")
                        j = v.get("Jour", "")
                        if y:
                            if m and j:
                                dob = f"{y}-{str(m).zfill(2)}-{str(j).zfill(2)}"
                            elif m:
                                dob = f"{y}-{str(m).zfill(2)}"
                            else:
                                dob = str(y)

                if not nom and not prenoms:
                    continue

                # Une entrée par prénom (comme dans screening.py)
                for prenom in (prenoms if prenoms else [""]):
                    premier = prenom.split()[0] if prenom else ""
                    self.entries.append({
                        'nom':            nom,
                        'prenom':         premier,
                        'prenom_complet': prenom,
                        'date_naissance': dob,
                        'nationalite':    [n for n in nationalities if n],
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

                    if nom or prenom:
                        premier = prenom.split()[0] if prenom else ""
                        self.entries.append({
                            'nom':            nom,
                            'prenom':         premier,
                            'prenom_complet': prenom,
                            'date_naissance': date,
                            'nationalite':    [nat] if nat else [],
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

        if best_score >= SEUIL_BLOCK:
            decision = "BLOCK"
            reason = f"Match GDA fort ({best_score})"
            alert = "GDA"
            match_name = f"{best_match.get('prenom', '')} {best_match.get('nom', '')}".strip()
        elif best_score >= SEUIL_REVIEW:
            decision = "REVIEW"
            reason = f"Match GDA possible ({best_score})"
            alert = "GDA"
            match_name = f"{best_match.get('prenom', '')} {best_match.get('nom', '')}".strip()

        # PPE
        if is_ppe:
            if alert == "GDA":
                alert = "BOTH"
                reason += f" + PPE ({ppe_score})"
            else:
                alert = "PPE"
                decision = "REVIEW"
                reason = f"PPE ({ppe_score})"

        if ppe_answers and any(ppe_answers.values()):
            if decision == "OK":
                decision = "REVIEW"
                alert = "PPE"

        return {
            "decision": decision,
            "score": best_score,
            "gda_score": best_score,
            "gda_match": best_score >= SEUIL_REVIEW,
            "decision_reason": reason,
            "alert_type": alert,
            "match_name": match_name,
            "matched_fields": matched_fields,
            "can_subscribe": decision == "OK"
        }


if __name__ == "__main__":
    engine = ScreeningEngine()

    if engine.is_ready():
        print(f"✅ Prêt - {len(engine.entries)} entrées")