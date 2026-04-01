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
if 'step' not in st.session_state:
    st.session_state.step = 1
if 'form_data' not in st.session_state:
    st.session_state.form_data = {}
if 'api_last_update' not in st.session_state:
    st.session_state.api_last_update = None

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
                    🚫 <strong>BLOCAGE AUTOMATIQUE</strong><br>
                    Correspondance détectée avec : <strong>{matched_name}</strong> (score: {score})<br>
                    ❌ Impossible de poursuivre la souscription.
                </div>
                """, unsafe_allow_html=True)

                # Afficher détails en déroulé
                if matched_person_details:
                    with st.expander("📋 Détails de la personne détectée", expanded=False):
                        col_a, col_b = st.columns(2)
                        with col_a:
                            st.markdown(f"**Prénom complet :** {matched_person_details.get('prenom_complet', 'N/A')}")
                            st.markdown(f"**Nom :** {matched_person_details.get('nom', 'N/A')}")
                        with col_b:
                            st.markdown(f"**Date naissance :** {matched_person_details.get('date_naissance', 'N/A')}")
                            nats = matched_person_details.get('nationalite', [])
                            nat_display = ', '.join(nats) if nats else 'N/A'
                            st.markdown(f"**Nationalité :** {nat_display}")

            elif is_match and score >= 50:
                st.markdown(f"""
                <div class="alert alert-warning">
                    ⚠️ <strong>ATTENTION</strong> : Correspondance possible détectée !<br>
                    <strong>{matched_name}</strong> (score: {score})<br>
                    Vérifiez attentivement avant de continuer.
                </div>
                """, unsafe_allow_html=True)

        # VÉRIFIER AUSSI LA NATIONALITÉ EN TEMPS RÉEL
        if nationality and not detection_bloquante:
            # Normaliser et vérifier
            from screening_engine import norm, SENSITIVE_NATIONALITIES
            nat_norm = norm(nationality)

            for country, variations in SENSITIVE_NATIONALITIES.items():
                for variation in variations:
                    if norm(variation) in nat_norm:
                        detection_bloquante = True
                        st.markdown(f"""
                        <div class="alert alert-danger">
                            🚫 <strong>BLOCAGE AUTOMATIQUE - Nationalité sensible</strong><br>
                            Nationalité détectée : <strong>{nationality}</strong><br>
                            ❌ Cette nationalité nécessite une validation manuelle obligatoire.
                        </div>
                        """, unsafe_allow_html=True)
                        break
                if detection_bloquante:
                    break

        st.caption("💡 _La nationalité et date de naissance permettent d'éviter les homonymes et de détecter les nationalités sensibles (Russie, Chine, Iran, etc.)_")

        st.markdown("</div>", unsafe_allow_html=True)

        col_btn = st.columns([1, 2, 1])
        with col_btn[1]:
            # Désactiver le bouton si détection bloquante
            button_clicked = st.button("🔍 Vérifier GDA + Nationalité", use_container_width=True, disabled=detection_bloquante)

            # Message si tentative malgré blocage
            if detection_bloquante:
                st.caption("⚠️ _Impossible de continuer : correspondance GDA détectée_")

            # NE PAS EXÉCUTER si détection bloquante, même si le bouton est cliqué
            if button_clicked and not detection_bloquante:
                if first_name and last_name:
                    # Préparer les données
                    client_data = {
                        "first_name": first_name,
                        "last_name": last_name,
                        "birth_date": birth_date.isoformat() if birth_date else None,
                        "nationality": nationality if nationality else None
                    }

                    # UTILISER L'ALGORITHME EXACT du screening_engine.py
                    # Le fichier JSON est déjà chargé dans screening_engine
                    result = st.session_state.screening_engine.screen_client(client_data)

                    # Afficher selon la décision
                    if result["decision"] == "BLOCK":
                        st.markdown(f"""
                        <div class="alert alert-danger">
                            <strong>🚨 ALERTE CRITIQUE - GDA</strong><br>
                            Correspondance détectée (score: {result['gda_score']}%)<br>
                            {result['decision_reason']}<br>
                            <small>Email envoyé automatiquement</small>
                        </div>
                        """, unsafe_allow_html=True)

                    elif result["decision"] == "REVIEW" and result["alert_type"] == "NATIONALITY":
                        st.markdown(f"""
                        <div class="alert alert-warning">
                            <strong>⚠️ ALERTE VIGILANCE - Nationalité Sensible</strong><br>
                            Nationalité : {nationality}<br>
                            Risque : {result.get('nationality_risk', {}).get('risk_level', 'N/A')}<br>
                            {result['decision_reason']}<br>
                            <small>⏸️ Dossier en pause - Validation Référente requise (24h)</small>
                        </div>
                        """, unsafe_allow_html=True)

                    else:
                        st.markdown("""
                        <div class="alert alert-success">
                            <strong>✓ GDA et Nationalité : OK</strong><br>
                            Aucune correspondance GDA • Nationalité standard
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
            <strong>✓ Étape 1 validée</strong> - GDA et Nationalité OK
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
            if st.button("← Retour"):
                st.session_state.step = 1
                st.rerun()
        with cols[2]:
            if st.button("Continuer", use_container_width=True):
                if profession:
                    client_data = {**st.session_state.form_data, "profession": profession}

                    # ALGORITHME EXACT
                    result = st.session_state.screening_engine.screen_client(client_data)

                    if result["decision"] == "REVIEW" and result["alert_type"] == "PPE":
                        st.markdown(f"""
                        <div class="alert alert-warning">
                            <strong>⚠️ ALERTE PPE RENFORCÉ</strong><br>
                            Fonction détectée : {', '.join(result.get('ppe_keywords', []))}<br>
                            {result['decision_reason']}<br>
                            <small>⏸️ Validation Référente requise (48h)</small>
                        </div>
                        """, unsafe_allow_html=True)

                    elif result["decision"] == "PENDING_CLARIFICATION":
                        st.session_state.form_data["profession"] = profession
                        st.session_state.form_data["pending_clarification"] = True
                        st.rerun()

                    else:
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

        if st.button("✓ Confirmer"):
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
                    st.markdown(f"""
                    <div class="alert alert-warning">
                        <strong>⚠️ ALERTE PPE RENFORCÉ</strong><br>
                        {result['decision_reason']}<br>
                        <small>⏸️ Validation Référente requise (48h)</small>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.session_state.form_data["ppe_answers"] = ppe_answers
                    st.session_state.step = 4
                    time.sleep(0.3)
                    st.rerun()

    # ÉTAPE 4 : Finalisation
    elif st.session_state.step == 4:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("""
        <div class="alert alert-success">
            <strong>✓ Vérification complète validée</strong><br>
            GDA ✓ • Nationalité ✓ • PPE Métier ✓ • Questions PPE ✓
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

                        if st.button("🔄 Nouvelle souscription"):
                            st.session_state.step = 1
                            st.session_state.form_data = {}
                            st.rerun()
                else:
                    st.error("⚠️ Veuillez remplir tous les champs obligatoires")

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
        page = st.radio("Choisir une page", ["Souscrire", "Clients", "Statistiques"], label_visibility="collapsed")

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
        st.markdown("### 📋 REGISTRE GDA")

        if st.session_state.screening_engine.is_ready():
            nb_entries = len(st.session_state.screening_engine.entries)
            source = getattr(st.session_state.screening_engine, 'source', 'Inconnue')

            st.success(f"✅ {nb_entries} entrées")
            st.caption(f"Source: {source}")

            # Bouton pour voir échantillon
            if st.button("🔍 Voir échantillon (50)", use_container_width=True):
                with st.expander("📋 Échantillon du registre", expanded=True):
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

if __name__ == "__main__":
    main()