#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Application Streamlit - Screening GDA/PPE v4.0
ALGORITHME EXACT + API France + Date MAJ + Optimisé
"""

import streamlit as st
from datetime import datetime
from typing import Optional, Dict, Any
import time
import requests
import json
import os
from pathlib import Path

# Configuration
st.set_page_config(
    page_title="Screening GDA/PPE",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import des modules
from screening_engine import ScreeningEngine
from database import Database

# Initialisation
if 'db' not in st.session_state:
    st.session_state.db = Database()
if 'screening_engine' not in st.session_state:
    st.session_state.screening_engine = ScreeningEngine()
if 'sanctions_loader' not in st.session_state:
    from screening_engine import get_sanctions_loader
    st.session_state.sanctions_loader = get_sanctions_loader()
if 'step' not in st.session_state:
    st.session_state.step = 1
if 'form_data' not in st.session_state:
    st.session_state.form_data = {}
if 'api_last_update' not in st.session_state:
    st.session_state.api_last_update = None
if 'nationality_alert' not in st.session_state:
    st.session_state.nationality_alert = None
if 'current_search_id' not in st.session_state:
    st.session_state.current_search_id = None  # ID de la recherche en cours dans l'historique

# CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    
    :root {
        --primary: #1e3a8a;
        --success: #10b981;
        --warning: #f59e0b;
        --danger: #ef4444;
    }
    
    * { font-family: 'Inter', sans-serif !important; }
    
    .main { background: #f8fafc; padding: 2rem; }
    
    .main-header {
        background: linear-gradient(135deg, var(--primary) 0%, #3b82f6 100%);
        padding: 2.5rem 3rem;
        border-radius: 16px;
        margin-bottom: 3rem;
        box-shadow: 0 10px 30px rgba(30, 58, 138, 0.2);
    }
    
    .main-header h1 {
        color: #FFFFFF !important;
        font-size: 2.25rem;
        font-weight: 700;
        margin: 0 0 0.5rem 0;
    }
    
    .main-header p {
        color: rgba(255, 255, 255, 0.95) !important;
        font-size: 1rem;
        margin: 0;
    }
    
    .card {
        background: white;
        border-radius: 12px;
        padding: 2rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        border: 1px solid #e2e8f0;
        margin-bottom: 2rem;
    }
    
    .alert {
        padding: 1rem 1.25rem;
        border-radius: 10px;
        margin: 1.5rem 0;
        border-left: 3px solid;
    }
    
    .alert-info { background: #eff6ff; border-color: #3b82f6; color: #1e40af; }
    .alert-success { background: #f0fdf4; border-color: var(--success); color: #166534; }
    .alert-warning { background: #fffbeb; border-color: var(--warning); color: #92400e; }
    .alert-danger { background: #fef2f2; border-color: #ef4444; color: #991b1b; font-weight: 600; }
    .alert-danger { background: #fef2f2; border-color: var(--danger); color: #991b1b; }
    
    .progress-dots {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 0.75rem;
        margin: 2rem 0;
    }
    
    .dot {
        width: 10px;
        height: 10px;
        border-radius: 50%;
        background: #e2e8f0;
        transition: all 0.3s;
    }
    
    .dot.active {
        width: 32px;
        border-radius: 5px;
        background: var(--primary);
    }
    
    .dot.done { background: var(--success); }
    .dot-line { width: 30px; height: 2px; background: #e2e8f0; }
    
    .stButton > button {
        background: var(--primary) !important;
        color: white !important;
        border-radius: 10px !important;
        padding: 0.65rem 1.5rem !important;
        font-weight: 500 !important;
        transition: all 0.2s !important;
    }
    
    .stButton > button:hover {
        background: #3b82f6 !important;
        transform: translateY(-1px);
    }
    
    .stTextInput > div > div > input,
    .stSelectbox > div > div > select,
    .stDateInput > div > div > input {
        border-radius: 10px !important;
        border: 1.5px solid #e2e8f0 !important;
        padding: 0.65rem 1rem !important;
    }
    
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, var(--primary) 0%, #1e40af 100%);
    }
    
    [data-testid="stSidebar"] * { color: white !important; }
    
    [data-testid="stSidebar"] .stMarkdown h3 {
        font-size: 0.95rem !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
        margin-bottom: 1rem !important;
    }
    
    .alert-nationality-noire {
        background: #fff1f2;
        border: 2px solid #e11d48;
        border-radius: 10px;
        padding: 1rem 1.25rem;
        margin: 0.75rem 0;
        color: #881337;
    }
    .alert-nationality-sanctions {
        background: #fff7ed;
        border: 2px solid #ea580c;
        border-radius: 10px;
        padding: 1rem 1.25rem;
        margin: 0.75rem 0;
        color: #7c2d12;
    }
    .alert-nationality-grise {
        background: #fffbeb;
        border: 2px solid #d97706;
        border-radius: 10px;
        padding: 1rem 1.25rem;
        margin: 0.75rem 0;
        color: #92400e;
    }
    .badge {
        display: inline-block;
        padding: 0.15rem 0.5rem;
        border-radius: 999px;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        margin-right: 0.4rem;
    }
    .badge-rouge { background: #fecdd3; color: #9f1239; }
    .badge-orange { background: #fed7aa; color: #9a3412; }
    .badge-jaune { background: #fef08a; color: #713f12; }
    .gda-detail-box {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        padding: 1rem;
        margin-top: 0.75rem;
        font-size: 0.88rem;
    }
    .api-status {
        background: rgba(255,255,255,0.1);
        padding: 0.75rem;
        border-radius: 8px;
        margin: 0.5rem 0;
    }
    
    .api-status-date {
        font-size: 0.8rem;
        opacity: 0.8;
        margin-top: 0.25rem;
    }
</style>
""", unsafe_allow_html=True)

def render_nationality_alert_banner():
    """
    Affiche l'alerte nationalité persistante (si présente) à TOUTES les étapes.
    L'alerte est informative — elle ne bloque PAS le parcours.
    """
    alert = st.session_state.get("nationality_alert")
    if not alert:
        return

    rl = alert.get("risk_level", "")
    label = alert.get("label", "")
    action = alert.get("action", "")
    source = alert.get("source", "")
    rationale = alert.get("rationale", "")
    fatf_date = alert.get("fatf_date", "")

    if rl == "LISTE_NOIRE":
        css_class = "alert-nationality-noire"
        badge_class = "badge-rouge"
        icon = "🔴"
        titre = "LISTE NOIRE FATF — Contre-mesures obligatoires"
        action_label = "CONTRE-MESURES"
    elif rl == "SANCTIONS_UE":
        css_class = "alert-nationality-sanctions"
        badge_class = "badge-orange"
        icon = "🟠"
        titre = "SANCTIONS UE / DG TRÉSOR — Diligence renforcée"
        action_label = "DDR RENFORCÉE"
    else:
        css_class = "alert-nationality-grise"
        badge_class = "badge-jaune"
        icon = "🟡"
        titre = "LISTE GRISE FATF — Vigilance renforcée"
        action_label = "VIGILANCE RENFORCÉE"

    date_str = f" (depuis {fatf_date})" if fatf_date else ""

    st.markdown(f"""
    <div class="{css_class}">
        <div style="display:flex; align-items:center; gap:0.5rem; margin-bottom:0.5rem;">
            <span style="font-size:1.1rem">{icon}</span>
            <strong>⚠️ ALERTE NATIONALITÉ PERSISTANTE</strong>
            <span class="badge {badge_class}">{action_label}</span>
        </div>
        <div><strong>Pays concerné :</strong> {label}{date_str}</div>
        <div style="margin-top:0.4rem; font-size:0.88rem;"><strong>Motif :</strong> {rationale}</div>
        <div style="margin-top:0.4rem; font-size:0.82rem; opacity:0.85;"><strong>Base légale :</strong> {source}</div>
        <div style="margin-top:0.5rem; padding:0.4rem 0.75rem; background:rgba(0,0,0,0.04); border-radius:6px; font-size:0.82rem;">
            ℹ️ <em>La nationalité d'un pays à risque ne signifie pas que la personne est elle-même sanctionnée.
            Ce dossier peut être poursuivi avec une diligence renforcée obligatoire (DDR).
            Un rapport de sûreté doit être conservé au dossier.</em>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_gda_details(details: dict):
    """Affiche les détails enrichis d'un match GDA — colonnes exactes du registre."""
    if not details:
        return

    nats          = ', '.join(details.get('nationalite', [])) or '—'
    aliases       = ', '.join(details.get('alias', [])) or '—'
    passeports    = ', '.join(details.get('passeport', [])) or '—'
    identifications = ', '.join(details.get('identification', [])) or '—'
    adresses      = ' | '.join(details.get('adresse', [])) or '—'
    champs        = ', '.join(details.get('champs_correspondants', [])) or 'Nom / Prénom'

    id_reg        = details.get('id_registre') or '—'
    regime        = details.get('regime') or '—'
    titre         = details.get('titre') or '—'
    date_naiss    = details.get('date_naissance') or '—'
    lieu_naiss    = details.get('lieu_naissance') or '—'
    fondement     = details.get('fondement_jur') or '—'
    motifs        = details.get('motifs') or '—'
    source        = details.get('source', 'Inconnue')

    with st.expander("Détails complets — Registre GDA (DG Trésor)", expanded=True):
        st.markdown(f"""
        <div class="gda-detail-box">
            <table>
                <tr><td>🆔 Id registre</td><td>{id_reg}</td></tr>
                <tr><td>🏛️ Régime</td><td><strong>{regime}</strong></td></tr>
                <tr><td>👤 Nom complet</td><td><strong>{details.get('nom_complet', '—')}</strong></td></tr>
                <tr><td>🏷️ Alias / Autres noms</td><td>{aliases}</td></tr>
                <tr><td>🎖️ Titre</td><td>{titre}</td></tr>
                <tr><td>🎂 Date de naissance</td><td>{date_naiss}</td></tr>
                <tr><td>📍 Lieu de naissance</td><td>{lieu_naiss}</td></tr>
                <tr><td>🌍 Nationalité(s)</td><td>{nats}</td></tr>
                <tr><td>🏠 Adresse</td><td>{adresses}</td></tr>
                <tr><td>🛂 Passeport(s)</td><td>{passeports}</td></tr>
                <tr><td>🪪 Identification</td><td>{identifications}</td></tr>
                <tr><td>⚖️ Fondement juridique</td><td>{fondement}</td></tr>
                <tr><td>📝 Motifs</td><td>{motifs}</td></tr>
                <tr><td>🔗 Source</td><td>{source}</td></tr>
                <tr><td>✅ Champs correspondants</td><td>{champs}</td></tr>
            </table>
        </div>
        """, unsafe_allow_html=True)
        st.caption("⚠️ Informations issues du Registre National des Gels d'Avoirs. Strictement réservées à l'usage interne — ne pas divulguer au client.")


def get_register_last_update():
    """Récupère la date de dernière mise à jour du registre"""
    try:
        # Si screening_engine a la méthode
        if hasattr(st.session_state.screening_engine, 'get_last_update_info'):
            return st.session_state.screening_engine.get_last_update_info()

        # Sinon tenter le fichier local
        register_path = Path(__file__).parent / "data" / "Registrenationaldesgels(1).json"
        if register_path.exists():
            mtime = register_path.stat().st_mtime
            return datetime.fromtimestamp(mtime).strftime("%d/%m/%Y à %H:%M")

        return "Non disponible"
    except Exception as e:
        return f"Erreur: {str(e)}"

def render_header():
    """Header"""
    engine = st.session_state.get("screening_engine")
    source = getattr(engine, 'source', None) if engine else None
    src_info = "🇫🇷 API France" if source == "France" else ("🇲🇨 API Monaco (repli)" if source == "Monaco" else "Source inconnue")

    st.markdown(f"""
    <div class="main-header">
        <h1>🛡️ Screening GDA/PPE</h1>
        <p>Système conforme ACPR v4.0 • {src_info}</p>
    </div>
    """, unsafe_allow_html=True)

def render_progress(step: int):
    """Progression"""
    html = '<div class="progress-dots">'
    for i in range(1, 5):
        status = "done" if i < step else "active" if i == step else ""
        html += f'<div class="dot {status}"></div>'
        if i < 4:
            html += '<div class="dot-line"></div>'
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)

def render_subscription_form():
    """Formulaire de souscription avec ALGORITHME EXACT"""

    render_progress(st.session_state.step)

    # ── BANNIÈRE ALERTE NATIONALITÉ PERSISTANTE ──────────────────────────────
    render_nationality_alert_banner()

    # ÉTAPE 1 : GDA
    if st.session_state.step == 1:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("### 🔍 Étape 1 : Vérification GDA")
        st.markdown("""
        <div class="alert alert-info">
            <strong>Contrôle prioritaire</strong><br>
            Vérification contre le Registre National des Gels d'Avoirs
        </div>
        """, unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            first_name = st.text_input("Prénom *", key="first_name", label_visibility="visible")
        with col2:
            last_name = st.text_input("Nom *", key="last_name", label_visibility="visible")

        col3, col4 = st.columns(2)
        with col3:
            birth_date = st.date_input("Date de naissance (recommandée)", value=None, key="birth_date", label_visibility="visible", format="DD/MM/YYYY")
        with col4:
            nationality = st.text_input("Nationalité (recommandée)", key="nationality", placeholder="Française", label_visibility="visible")

        # DÉTECTION EN TEMPS RÉEL (APRÈS avoir défini tous les champs)
        detection_bloquante = False
        matched_person_details = None

        if first_name or last_name:
            is_match, score, matched_name = st.session_state.screening_engine.check_gda_live(first_name, last_name)

            if is_match and score >= 70:
                detection_bloquante = True

                # Récupérer les détails complets de la personne
                for entry in st.session_state.screening_engine.entries:
                    entry_full = f"{entry.get('prenom_complet', entry.get('prenom', ''))} {entry.get('nom', '')}".strip()
                    if matched_name.lower() in entry_full.lower() or entry_full.lower() in matched_name.lower():
                        matched_person_details = entry
                        break

                st.markdown(f"""
                <div class="alert alert-danger">
                    🚫 <strong>BLOCAGE AUTOMATIQUE — CORRESPONDANCE GDA DÉTECTÉE</strong><br>
                    Similarité avec : <strong>{matched_name}</strong> (score : {score}/100)<br>
                    ❌ Souscription impossible — vérification manuelle obligatoire.
                </div>
                """, unsafe_allow_html=True)

                # Détails enrichis — colonnes exactes du registre
                if matched_person_details:
                    nats    = ', '.join(matched_person_details.get('nationalite', [])) or '—'
                    aliases = ', '.join(matched_person_details.get('alias', [])) or '—'
                    ppts    = ', '.join(matched_person_details.get('passeport', [])) or '—'
                    ids     = ', '.join(matched_person_details.get('identification', [])) or '—'
                    addrs   = ' | '.join(matched_person_details.get('adresse', [])) or '—'
                    motifs  = matched_person_details.get('motifs') or '—'
                    fond    = matched_person_details.get('fondement_jur') or '—'
                    regime  = matched_person_details.get('regime') or '—'
                    titre   = matched_person_details.get('titre') or '—'
                    id_reg  = matched_person_details.get('id_registre') or '—'
                    lieu    = matched_person_details.get('lieu_naissance') or '—'
                    ddn     = matched_person_details.get('date_naissance') or '—'
                    nom_aff = f"{matched_person_details.get('prenom_complet', matched_person_details.get('prenom', ''))} {matched_person_details.get('nom', '')}".strip()

                    with st.expander("Détails — Registre GDA (DG Trésor)", expanded=True):
                        st.markdown(f"""
                        <div class="gda-detail-box">
                            <table>
                                <tr><td>🆔 Id registre</td><td>{id_reg}</td></tr>
                                <tr><td>🏛️ Régime</td><td><strong>{regime}</strong></td></tr>
                                <tr><td>👤 Nom complet</td><td><strong>{nom_aff}</strong></td></tr>
                                <tr><td>🏷️ Alias / Autres noms</td><td>{aliases}</td></tr>
                                <tr><td>🎖️ Titre</td><td>{titre}</td></tr>
                                <tr><td>🎂 Date de naissance</td><td>{ddn}</td></tr>
                                <tr><td>📍 Lieu de naissance</td><td>{lieu}</td></tr>
                                <tr><td>🌍 Nationalité(s)</td><td>{nats}</td></tr>
                                <tr><td>🏠 Adresse</td><td>{addrs}</td></tr>
                                <tr><td>🛂 Passeport(s)</td><td>{ppts}</td></tr>
                                <tr><td>🪪 Identification</td><td>{ids}</td></tr>
                                <tr><td>⚖️ Fondement juridique</td><td>{fond}</td></tr>
                                <tr><td>📝 Motifs</td><td>{motifs}</td></tr>
                            </table>
                        </div>
                        """, unsafe_allow_html=True)
                        st.caption("⚠️ Informations issues du Registre National des Gels d'Avoirs. Strictement réservées à l'usage interne — ne pas divulguer au client.")

            elif is_match and score >= 50:
                st.markdown(f"""
                <div class="alert alert-warning">
                    ⚠️ <strong>ATTENTION</strong> : Correspondance possible dans le registre GDA<br>
                    <strong>{matched_name}</strong> (score : {score}/100)<br>
                    Vérifiez attentivement les informations avant de continuer.
                </div>
                """, unsafe_allow_html=True)

        # VÉRIFICATION NATIONALITÉ — non bloquante, alerte persistante
        if nationality:
            from screening_engine import get_nationality_risk
            nat_risk = get_nationality_risk(nationality)
            if nat_risk:
                # Stocker l'alerte en session pour la rendre persistante
                st.session_state.nationality_alert = nat_risk
                # Afficher immédiatement
                render_nationality_alert_banner()
            else:
                # Réinitialiser si nationalité non sensible
                if st.session_state.nationality_alert is not None:
                    st.session_state.nationality_alert = None

        st.caption("💡 _La nationalité et date de naissance permettent d'éviter les homonymes. Une nationalité à risque génère une alerte de diligence renforcée (DDR) — elle ne bloque pas la souscription._")

        st.markdown("</div>", unsafe_allow_html=True)

        col_btn = st.columns([1, 2, 1])
        with col_btn[1]:
            button_clicked = st.button("🔍 Vérifier GDA", use_container_width=True, disabled=detection_bloquante)

            if detection_bloquante:
                st.caption("⚠️ _Impossible de continuer : correspondance GDA détectée — contact référent obligatoire_")

                # ── Log du blocage live au moment où les deux champs sont complets ──
                # Le bouton étant désactivé, on log ici avec un flag anti-doublon
                if first_name and last_name and matched_person_details is not None:
                    live_key = f"live_logged_{first_name.strip().lower()}_{last_name.strip().lower()}"
                    if not st.session_state.get(live_key):
                        st.session_state[live_key] = True
                        sid = st.session_state.db.log_search({
                            "first_name":      first_name,
                            "last_name":       last_name,
                            "birth_date":      birth_date.isoformat() if birth_date else "",
                            "nationality":     nationality or "",
                            "profession":      "",
                            "gda_decision":    "BLOCK",
                            "gda_score":       score,
                            "gda_details":     {
                                "nom_complet":   matched_name,
                                "fondement_jur": matched_person_details.get('fondement_jur', ''),
                                "motifs":        matched_person_details.get('motifs', ''),
                            },
                            "ppe_detected":    False,
                            "ppe_keywords":    [],
                            "nationality_risk": None,
                            "final_decision":  "BLOCK",
                            "decision_reason": f"Correspondance GDA : {matched_name} (score {score}/100)",
                            "outcome":         "BLOQUE",
                        })
                        st.session_state.current_search_id = sid

            if button_clicked and not detection_bloquante:
                if first_name and last_name:
                    client_data = {
                        "first_name": first_name,
                        "last_name": last_name,
                        "birth_date": birth_date.isoformat() if birth_date else None,
                        "nationality": nationality if nationality else None
                    }

                    result = st.session_state.screening_engine.screen_client(client_data)

                    # Mettre à jour l'alerte nationalité en session
                    if result.get("nationality_risk"):
                        st.session_state.nationality_alert = result["nationality_risk"]
                    else:
                        st.session_state.nationality_alert = None

                    # ── Enregistrement dans l'historique ─────────────────────
                    decision = result["decision"]
                    if decision == "BLOCK":
                        outcome = "BLOQUE"
                    elif decision == "REVIEW":
                        outcome = "REVIEW"
                    else:
                        outcome = "EN_COURS"  # Pas encore terminé — continue vers PPE

                    search_id = st.session_state.db.log_search({
                        "first_name":      first_name,
                        "last_name":       last_name,
                        "birth_date":      birth_date.isoformat() if birth_date else "",
                        "nationality":     nationality or "",
                        "profession":      "",
                        "gda_decision":    decision,
                        "gda_score":       result.get("gda_score", 0),
                        "gda_details":     result.get("gda_details"),
                        "ppe_detected":    False,
                        "ppe_keywords":    [],
                        "nationality_risk": result.get("nationality_risk"),
                        "final_decision":  decision,
                        "decision_reason": result.get("decision_reason", ""),
                        "outcome":         outcome,
                    })
                    st.session_state.current_search_id = search_id

                    if result["decision"] == "BLOCK":
                        gda_details = result.get("gda_details")
                        st.markdown(f"""
                        <div class="alert alert-danger">
                            <strong>🚨 BLOCAGE GDA — CORRESPONDANCE FORTE</strong><br>
                            {result['decision_reason']}<br>
                            <small>Rapport de blocage à transmettre au référent LCB-FT</small>
                        </div>
                        """, unsafe_allow_html=True)
                        render_gda_details(gda_details)

                    elif result["decision"] == "REVIEW" and result.get("alert_type") == "GDA":
                        gda_details = result.get("gda_details")
                        st.markdown(f"""
                        <div class="alert alert-warning">
                            <strong>⚠️ CORRESPONDANCE GDA POSSIBLE — Vérification requise</strong><br>
                            {result['decision_reason']}<br>
                            <small>Vérifiez les détails ci-dessous avant de décider</small>
                        </div>
                        """, unsafe_allow_html=True)
                        render_gda_details(gda_details)

                    else:
                        # Alerte nationalité si présente (non bloquante)
                        if result.get("nationality_risk"):
                            render_nationality_alert_banner()

                        st.markdown("""
                        <div class="alert alert-success">
                            <strong>✓ Contrôle GDA : aucune correspondance</strong><br>
                            Passage à l'étape suivante autorisé
                        </div>
                        """, unsafe_allow_html=True)

                        st.session_state.form_data = client_data
                        st.session_state.step = 2
                        time.sleep(0.3)
                        st.rerun()
                else:
                    st.error("⚠️ Veuillez remplir au minimum : Prénom et Nom")

    # ÉTAPE 2 : PPE Profession
    elif st.session_state.step == 2:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("""
        <div class="alert alert-success">
            <strong>✓ Étape 1 validée</strong> — Contrôle GDA : aucune correspondance
        </div>
        """, unsafe_allow_html=True)

        st.markdown("### 👔 Étape 2 : Vérification PPE Profession")

        profession = st.text_input("Profession ou fonction *", key="profession",
                                   placeholder="Ex: Ingénieur, Consultant, Député...",
                                   label_visibility="visible")
        st.caption("Détection automatique : ministre, député, maire, ambassadeur, etc.")

        st.markdown("</div>", unsafe_allow_html=True)

        cols = st.columns([1, 1, 1])
        with cols[0]:
            if st.button("← Retour", key="back2"):
                st.session_state.step = 1
                st.rerun()
        with cols[2]:
            if st.button("Continuer", use_container_width=True, key="continuer2"):
                if profession:
                    client_data = {**st.session_state.form_data, "profession": profession}

                    result = st.session_state.screening_engine.screen_client(client_data)

                    if result["decision"] == "REVIEW" and result["alert_type"] == "PPE":
                        kws = ', '.join(result.get('ppe_keywords', [])) or 'N/A'
                        ppe_score = result.get('ppe_score', 0)
                        # Mise à jour historique — PPE détecté à l'étape profession
                        st.session_state.db.update_search(st.session_state.current_search_id, {
                            "profession":      profession,
                            "ppe_detected":    True,
                            "ppe_keywords":    ', '.join(result.get('ppe_keywords', [])),
                            "final_decision":  "REVIEW",
                            "decision_reason": result['decision_reason'],
                            "outcome":         "REVIEW_PPE",
                        })
                        st.markdown(f"""
                        <div class="alert alert-warning">
                            <strong>⚠️ ALERTE PPE — Fonction à risque détectée</strong><br>
                            <strong>Fonction(s) identifiée(s) :</strong> {kws}<br>
                            <strong>Score de risque PPE :</strong> {ppe_score}/100<br>
                            <strong>Motif :</strong> {result['decision_reason']}<br>
                            <small>⏸️ Validation Référente LCB-FT requise sous 48h</small>
                        </div>
                        """, unsafe_allow_html=True)

                    elif result["decision"] == "PENDING_CLARIFICATION":
                        st.session_state.form_data["profession"] = profession
                        st.session_state.form_data["pending_clarification"] = True
                        st.rerun()

                    else:
                        # Mise à jour historique — profession OK, on continue
                        st.session_state.db.update_search(st.session_state.current_search_id, {
                            "profession":   profession,
                            "ppe_detected": False,
                            "outcome":      "EN_COURS",
                        })
                        st.session_state.form_data["profession"] = profession
                        st.session_state.step = 3
                        st.rerun()
                else:
                    st.error("⚠️ Veuillez renseigner votre profession")

    if st.session_state.form_data.get("pending_clarification"):
        client_data = {**st.session_state.form_data}
        st.markdown("""
        <div class="alert alert-warning">
            <strong>❓ Clarification requise</strong><br>
            Profession ambiguë : précisez si secteur public ou privé
        </div>
        """, unsafe_allow_html=True)

        is_public = st.radio("Cette fonction est-elle dans le secteur public ?",
                             ["Non", "Oui"], key="is_public", horizontal=True)

        if st.button("✓ Confirmer", key="confirmer_public"):
            result = st.session_state.screening_engine.screen_client(
                client_data,
                is_public_sector=(is_public == "Oui")
            )
            st.session_state.form_data["is_public_sector"] = (is_public == "Oui")
            st.session_state.form_data.pop("pending_clarification", None)

            if result["decision"] == "REVIEW":
                st.warning("⚠️ ALERTE PPE - Secteur public confirmé")
            else:
                st.session_state.step = 3
                st.rerun()


    # ÉTAPE 3 : Questions PPE (TOUJOURS OBLIGATOIRES)
    elif st.session_state.step == 3:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("""
        <div class="alert alert-success">
            <strong>✓ Étapes 1 et 2 validées</strong>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("### 📋 Étape 3 : Questions PPE (OBLIGATOIRES)")
        st.caption("Ces questions sont obligatoires pour tous les clients (réglementation ACPR)")

        st.markdown("<br>", unsafe_allow_html=True)

        q1 = st.checkbox("Je suis ou j'ai été une Personne Politiquement Exposée (PPE)", key="q1")
        st.caption("_Fonction politique, administrative ou juridictionnelle importante_")

        q2 = st.checkbox("J'ai cessé une fonction PPE depuis moins d'un an", key="q2")
        st.caption("_La réglementation couvre les 12 mois suivant la fin_")

        q3 = st.checkbox("Un membre de ma famille proche est une PPE", key="q3")
        st.caption("_Conjoint(e), enfants, parents_")

        q4 = st.checkbox("Je suis étroitement associé(e) à une PPE", key="q4")
        st.caption("_Lien d'affaires, société commune_")

        st.markdown("</div>", unsafe_allow_html=True)

        cols = st.columns([1, 1, 1])
        with cols[0]:
            if st.button("← Retour", key="back3"):
                st.session_state.step = 2
                st.rerun()
        with cols[2]:
            if st.button("Valider", use_container_width=True):
                ppe_answers = {
                    "is_ppe": q1,
                    "ceased_ppe_less_than_year": q2,
                    "close_relative_is_ppe": q3,
                    "closely_associated_to_ppe": q4
                }

                # ALGORITHME EXACT
                client_data = st.session_state.form_data.copy()
                result = st.session_state.screening_engine.screen_client(
                    client_data,
                    is_public_sector=client_data.get("is_public_sector"),
                    ppe_answers=ppe_answers
                )

                if result["decision"] == "REVIEW":
                    kws = ', '.join(result.get('ppe_keywords', [])) or 'N/A'
                    ppe_sc = result.get('ppe_score', 0)
                    ppe_flags = []
                    if q1: ppe_flags.append("PPE actuelle")
                    if q2: ppe_flags.append("PPE cessée < 1 an")
                    if q3: ppe_flags.append("Proche parent PPE")
                    if q4: ppe_flags.append("Associé proche d'une PPE")

                    # Mise à jour historique — bloqué en review PPE questions
                    declared = ", ".join(ppe_flags)
                    all_kws = list(filter(None, [kws if kws != "N/A" else "", declared]))
                    st.session_state.db.update_search(st.session_state.current_search_id, {
                        "ppe_detected":    True,
                        "ppe_keywords":    " | ".join(all_kws),
                        "final_decision":  "REVIEW",
                        "decision_reason": result['decision_reason'],
                        "outcome":         "REVIEW_PPE_QUESTIONS",
                    })

                    line_kws   = f"<strong>Fonctions à risque :</strong> {kws}<br>" if kws and kws != "N/A" else ""
                    line_flags = f"<strong>Déclarations PPE :</strong> {' | '.join(ppe_flags)}<br>" if ppe_flags else ""

                    st.markdown(f"""
                    <div class="alert alert-warning">
                        <strong>⚠️ ALERTE PPE RENFORCÉE — Validation obligatoire</strong><br>
                        <strong>Raison :</strong> {result['decision_reason']}<br>
                        {line_kws}{line_flags}<strong>Score PPE :</strong> {ppe_sc}/100<br>
                        <small>⏸️ Dossier suspendu — Validation Référente LCB-FT requise sous 48h</small>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    # Mise à jour historique — toutes les étapes OK, souscription autorisée
                    ppe_flags = []
                    if q1: ppe_flags.append("PPE actuelle")
                    if q2: ppe_flags.append("PPE cessée < 1 an")
                    if q3: ppe_flags.append("Proche parent PPE")
                    if q4: ppe_flags.append("Associé proche d'une PPE")
                    st.session_state.db.update_search(st.session_state.current_search_id, {
                        "ppe_detected":    any([q1, q2, q3, q4]),
                        "ppe_keywords":    ", ".join(ppe_flags) if ppe_flags else "",
                        "final_decision":  "OK",
                        "decision_reason": "GDA ✓ • PPE ✓ • Questions PPE ✓",
                        "outcome":         "VALIDE",
                    })
                    st.session_state.form_data["ppe_answers"] = ppe_answers
                    st.session_state.step = 4
                    time.sleep(0.3)
                    st.rerun()

    # ÉTAPE 4 : Finalisation
    elif st.session_state.step == 4:
        st.markdown('<div class="card">', unsafe_allow_html=True)

        # Résumé des contrôles
        nat_alert = st.session_state.get("nationality_alert")
        if nat_alert:
            rl = nat_alert.get("risk_level", "")
            action_label = {"LISTE_NOIRE": "DDR Renforcée (liste noire FATF)", "SANCTIONS_UE": "DDR Renforcée (sanctions UE)", "LISTE_GRISE": "Vigilance Renforcée (liste grise FATF)"}.get(rl, "DDR Renforcée")
            checks_html = f"GDA ✓ • Nationalité ⚠️ {action_label} • PPE Métier ✓ • Questions PPE ✓"
        else:
            checks_html = "GDA ✓ • Nationalité ✓ • PPE Métier ✓ • Questions PPE ✓"

        st.markdown(f"""
        <div class="alert alert-success">
            <strong>✓ Vérification complète</strong><br>
            {checks_html}
        </div>
        """, unsafe_allow_html=True)

        # Rappel alerte nationalité si présente
        if nat_alert:
            st.markdown(f"""
            <div class="alert alert-warning" style="font-size:0.88rem;">
                ⚠️ <strong>Rappel DDR :</strong> Ce dossier nécessite une diligence renforcée — nationalité à risque ({nat_alert.get('label', '')}). 
                Mentionner dans le rapport de souscription et conserver la justification au dossier.
            </div>
            """, unsafe_allow_html=True)

        st.markdown("### Informations complémentaires")

        col1, col2 = st.columns(2)
        with col1:
            email = st.text_input("Email *", key="email")
        with col2:
            phone = st.text_input("Téléphone", key="phone")

        st.markdown("### Détails de la souscription")

        col3, col4 = st.columns(2)
        with col3:
            scpi = st.selectbox("SCPI *", ["", "SCPI Patrimoine", "SCPI Corum Origin"], key="scpi")
        with col4:
            amount = st.number_input("Montant (€) *", min_value=1000, value=10000, key="amount")

        st.markdown("</div>", unsafe_allow_html=True)

        cols = st.columns([1, 1, 1])
        with cols[0]:
            if st.button("← Retour", key="back4"):
                st.session_state.step = 3
                st.rerun()
        with cols[2]:
            if st.button("✅ Valider la souscription", use_container_width=True):
                if email and scpi:
                    with st.spinner("Enregistrement..."):
                        full_data = {**st.session_state.form_data, "email": email, "phone_number": phone}

                        existing = st.session_state.db.search_client(
                            first_name=full_data["first_name"],
                            last_name=full_data["last_name"],
                            email=email
                        )

                        if existing:
                            client_id = existing["id"]
                        else:
                            client_id = st.session_state.db.create_client(full_data)

                        # Screening final
                        final_result = st.session_state.screening_engine.screen_client(
                            st.session_state.form_data,
                            is_public_sector=st.session_state.form_data.get("is_public_sector"),
                            ppe_answers=st.session_state.form_data.get("ppe_answers")
                        )

                        st.session_state.db.create_screening(client_id, final_result)

                        subscription_id = st.session_state.db.create_subscription(
                            client_id=client_id,
                            scpi_name=scpi,
                            amount=amount
                        )

                        st.success("✓ Souscription enregistrée")
                        st.balloons()

                        st.markdown(f"""
                        <div class="alert alert-success">
                            <strong>Souscription validée</strong><br>
                            ID: {subscription_id} • {scpi} • {amount:,.0f} €
                        </div>
                        """, unsafe_allow_html=True)

                        if st.button("🔄 Nouvelle souscription", key="nouvelle_souscription"):
                            st.session_state.step = 1
                            st.session_state.form_data = {}
                            st.rerun()
                else:
                    st.error("⚠️ Veuillez remplir tous les champs obligatoires")

def render_history():
    """Page historique des recherches avec export CSV."""
    st.markdown("## Historique des recherches")

    rows = st.session_state.db.get_search_history(limit=500)

    if not rows:
        st.info("Aucune recherche enregistrée pour l'instant.")
        return

    # ── Export CSV ────────────────────────────────────────────────────────────
    csv_data = st.session_state.db.export_search_history_csv()
    st.download_button(
        label="Exporter en CSV",
        data=csv_data.encode("utf-8-sig"),
        file_name=f"historique_screening_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        key="export_csv_history",
    )

    st.markdown(f"**{len(rows)} recherche(s) enregistrée(s)**")
    st.markdown("---")

    for row in rows:
        outcome = row.get("outcome", "")

        # Badge et couleurs selon outcome
        if outcome == "BLOQUE":
            badge        = "🔴 BLOQUÉ"
            color        = "#fef2f2"
            border       = "#ef4444"
            text_color   = "#991b1b"
        elif outcome in ("REVIEW", "REVIEW_PPE", "REVIEW_PPE_QUESTIONS"):
            badge        = "🟠 REVIEW"
            color        = "#fffbeb"
            border       = "#f59e0b"
            text_color   = "#92400e"
        elif outcome == "VALIDE":
            badge        = "🟢 VALIDÉ"
            color        = "#f0fdf4"
            border       = "#10b981"
            text_color   = "#166534"
        else:
            badge        = "⚪ EN COURS"
            color        = "#f8fafc"
            border       = "#94a3b8"
            text_color   = "#475569"

        # Infos complémentaires — pré-calculées hors HTML
        prenom        = row.get("first_name", "")
        nom           = row.get("last_name", "")
        date_str      = (row.get("created_at", "")[:16] or "").replace("T", " ")
        gda_match     = row.get("gda_match_name", "")
        nat_label     = row.get("nationality_risk_label", "")
        ppe_kws       = row.get("ppe_keywords", "")
        reason        = row.get("decision_reason", "") or "—"
        birth         = row.get("birth_date", "") or ""
        nationality   = row.get("nationality", "") or ""
        profession    = row.get("profession", "") or ""
        gda_score     = row.get("gda_score", 0) or 0

        # Ligne détails
        details_parts = [f"📅 {date_str}"]
        if gda_match:
            details_parts.append(f"GDA : {gda_match} (score {gda_score})")
        if nat_label:
            details_parts.append(f"⚠️ Nationalité : {nat_label}")
        if ppe_kws:
            details_parts.append(f"PPE : {ppe_kws}")
        details_str = "  •  ".join(details_parts)

        # Infos identité
        identity_parts = []
        if birth:
            identity_parts.append(f"DDN : {birth}")
        if nationality:
            identity_parts.append(f"Nationalité : {nationality}")
        if profession:
            identity_parts.append(f"Profession : {profession}")
        identity_str = "  •  ".join(identity_parts) if identity_parts else ""

        # Pré-calculer les lignes HTML conditionnelles
        identity_line = f"<div style='font-size:0.8rem;color:#94a3b8;margin-top:0.15rem;'>{identity_str}</div>" if identity_str else ""
        reason_line   = f"<div style='font-size:0.82rem;color:#374151;margin-top:0.35rem;padding-top:0.35rem;border-top:1px solid rgba(0,0,0,0.06);'>{reason}</div>"

        st.markdown(f"""
        <div style="background:{color};border-left:4px solid {border};border-radius:8px;
                    padding:0.85rem 1.1rem;margin-bottom:0.6rem;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.3rem;">
                <strong style="font-size:1rem;">{prenom} {nom}</strong>
                <span style="font-size:0.8rem;font-weight:700;color:{text_color};
                             background:white;padding:0.15rem 0.6rem;border-radius:999px;
                             border:1px solid {border};">{badge}</span>
            </div>
            <div style="font-size:0.8rem;color:#64748b;">{details_str}</div>
            {identity_line}
            {reason_line}
        </div>
        """, unsafe_allow_html=True)


def render_clients():
    """Clients"""
    st.markdown("## 👥 Clients")
    clients = st.session_state.db.get_clients_with_latest_screening()

    if clients:
        for client in clients:
            cols = st.columns([3, 2, 1])
            with cols[0]:
                st.markdown(f"**{client['first_name']} {client['last_name']}**")
            with cols[1]:
                st.caption(client.get('email', '—'))
            with cols[2]:
                decision = client.get('last_decision', 'N/A')
                if decision == 'OK':
                    st.success("✓ OK")
                elif decision == 'REVIEW':
                    st.warning("⚠ REVIEW")
                else:
                    st.error("✕ BLOCK")
            st.markdown("---")
    else:
        st.info("Aucun client")

def main():
    render_header()

    # Charger date MAJ registre une seule fois
    if st.session_state.api_last_update is None:
        st.session_state.api_last_update = get_register_last_update()

    with st.sidebar:
        st.markdown("### NAVIGATION")
        page = st.radio("Choisir une page", ["Souscrire", "Clients", "Historique", "Statistiques"], label_visibility="collapsed")

        st.markdown("---")

        # Bandeau source active (France ou Monaco selon disponibilité)
        engine = st.session_state.screening_engine
        source = getattr(engine, 'source', None)

        if source == "France":
            src_flag  = "🇫🇷"
            src_label = "API France — DG Trésor"
            src_color = "#10b981"
            src_icon  = "✅"
        elif source == "Monaco":
            src_flag  = "🇲🇨"
            src_label = "API Monaco (repli automatique)"
            src_color = "#f59e0b"
            src_icon  = "⚠️"
        else:
            src_flag  = "❓"
            src_label = "Source indisponible"
            src_color = "#ef4444"
            src_icon  = "❌"

        st.markdown(f"### {src_flag} API REGISTRE GDA")

        st.markdown(f"""
        <div class="api-status" style="border-left: 3px solid {src_color};">
            <div>{src_icon} {src_label}</div>
            <div class="api-status-date">
                📅 Dernière MAJ :<br>{st.session_state.api_last_update}
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("### 🌍 LISTES SANCTIONS (Live)")

        loader = st.session_state.get("sanctions_loader")
        if loader:
            status = loader.get_status_info()
            is_stale = status.get("is_stale", True)
            dgt_status = status.get("dgtresor_status", "—")
            fatf_status = status.get("fatf_status", "—")

            # Couleur selon fraîcheur des données
            color_dgt  = "#10b981" if "Live" in dgt_status  else "#f59e0b"
            color_fatf = "#10b981" if "Live" in fatf_status else "#f59e0b"

            st.markdown(f"""
            <div class="api-status" style="border-left: 3px solid {'#10b981' if not is_stale else '#f59e0b'};">
                <div style="font-size:0.82rem;">
                    🇫🇷 DG Trésor <span style="color:{color_dgt};">{dgt_status}</span><br>
                    &nbsp;&nbsp;→ {status['dgtresor_count']} pays sous sanctions<br>
                    🌐 FATF <span style="color:{color_fatf};">{fatf_status}</span><br>
                    &nbsp;&nbsp;→ 🔴 {status['fatf_black_count']} liste noire<br>
                    &nbsp;&nbsp;→ 🟡 {status['fatf_grey_count']} liste grise<br>
                    <strong>Total : {status['total_risk_countries']} pays à risque</strong>
                </div>
                <div class="api-status-date">
                    📅 Chargé : {status['last_loaded']}<br>
                    🔄 Prochain : {status['next_reload']}
                </div>
            </div>
            """, unsafe_allow_html=True)

            if st.button("🔄 Recharger sanctions", use_container_width=True, key="reload_sanctions"):
                with st.spinner("Rechargement DG Trésor + FATF…"):
                    loader.force_reload()
                    st.session_state.sanctions_loader = loader
                st.success("✅ Listes de sanctions rechargées")
                st.rerun()
        else:
            st.error("❌ Loader non initialisé")

        if st.session_state.screening_engine.is_ready():
            nb_entries = len(st.session_state.screening_engine.entries)
            source = getattr(st.session_state.screening_engine, 'source', 'Inconnue')

            st.success(f"✅ {nb_entries} entrées")
            st.caption(f"Source: {source}")

            # Bouton pour voir échantillon
            if st.button("🔍 Voir échantillon (50)", use_container_width=True, key="voir_echantillon"):
                with st.expander("Échantillon du registre", expanded=True):
                    for i, entry in enumerate(st.session_state.screening_engine.entries[:50], 1):
                        prenom = entry.get('prenom', '')
                        nom = entry.get('nom', '')
                        st.caption(f"{i:2d}. {prenom} {nom}")
        else:
            st.error("❌ Aucune donnée")

        st.markdown("---")
        st.markdown("### INFORMATIONS")
        st.caption("**Version** 4.0")
        st.caption("**Algorithme** ACPR")
        st.caption(f"**Date** {datetime.now().strftime('%d/%m/%Y')}")

        st.markdown("---")
        st.markdown("### 💡 ASTUCE")
        st.caption("En haut à droite, cliquez sur **⋮** puis **Settings** pour activer **'Always rerun'**")
        st.caption("L'app se rechargera automatiquement à chaque modification !")

    if page == "Souscrire":
        render_subscription_form()
    elif page == "Clients":
        render_clients()
    elif page == "Historique":
        render_history()

if __name__ == "__main__":
    main()
