#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module Database - Gestion de la base de données SQLite
"""

import sqlite3
import csv
import io
from datetime import datetime
from typing import Optional, List, Dict, Any
import json


class Database:
    def __init__(self, db_path: str = "screening.db"):
        self.db_path = db_path
        self.init_database()

    def get_connection(self):
        """Crée une connexion à la base de données"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_database(self):
        """Initialise les tables de la base de données"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Table clients
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                birth_date TEXT,
                nationality TEXT,
                profession TEXT,
                email TEXT,
                phone_number TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Table screenings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS screenings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                decision TEXT NOT NULL,
                decision_reason TEXT,
                gda_match INTEGER DEFAULT 0,
                ppe_risk INTEGER DEFAULT 0,
                screening_data TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            )
        """)

        # Table subscriptions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                scpi_name TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT DEFAULT 'ACTIVE',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            )
        """)

        # Table alerts
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                alert_type TEXT NOT NULL,
                severity TEXT DEFAULT 'MEDIUM',
                message TEXT,
                previous_status TEXT,
                new_status TEXT,
                is_read INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            )
        """)

        # Table search_history — historique de toutes les recherches (validées ou non)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS search_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                birth_date TEXT,
                nationality TEXT,
                profession TEXT,
                gda_decision TEXT,
                gda_score INTEGER,
                gda_match_name TEXT,
                gda_fondement TEXT,
                gda_motifs TEXT,
                ppe_detected INTEGER DEFAULT 0,
                ppe_keywords TEXT,
                nationality_risk_level TEXT,
                nationality_risk_label TEXT,
                nationality_risk_source TEXT,
                final_decision TEXT NOT NULL,
                decision_reason TEXT,
                outcome TEXT NOT NULL,
                operator_note TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        conn.close()

    # ── Historique des recherches ─────────────────────────────────────────────

    def log_search(self, search_data: Dict[str, Any]) -> int:
        """
        Enregistre une recherche dans l'historique.
        outcome : 'BLOQUE' | 'REVIEW' | 'VALIDE' | 'ABANDONNE'
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        nat_risk = search_data.get("nationality_risk") or {}
        gda_details = search_data.get("gda_details") or {}

        cursor.execute("""
            INSERT INTO search_history (
                first_name, last_name, birth_date, nationality, profession,
                gda_decision, gda_score, gda_match_name, gda_fondement, gda_motifs,
                ppe_detected, ppe_keywords,
                nationality_risk_level, nationality_risk_label, nationality_risk_source,
                final_decision, decision_reason, outcome, operator_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            search_data.get("first_name", ""),
            search_data.get("last_name", ""),
            search_data.get("birth_date", ""),
            search_data.get("nationality", ""),
            search_data.get("profession", ""),
            search_data.get("gda_decision", ""),
            search_data.get("gda_score", 0),
            gda_details.get("nom_complet", ""),
            gda_details.get("fondement_jur", ""),
            gda_details.get("motifs", ""),
            1 if search_data.get("ppe_detected") else 0,
            ", ".join(search_data.get("ppe_keywords", [])),
            nat_risk.get("risk_level", ""),
            nat_risk.get("label", ""),
            nat_risk.get("source", ""),
            search_data.get("final_decision", ""),
            search_data.get("decision_reason", ""),
            search_data.get("outcome", ""),
            search_data.get("operator_note", ""),
        ))

        search_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return search_id

    def get_search_history(self, limit: int = 200) -> List[Dict[str, Any]]:
        """Récupère l'historique des recherches, plus récentes en premier."""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM search_history
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def export_search_history_csv(self) -> str:
        """Exporte tout l'historique en CSV (retourne une chaîne UTF-8)."""
        rows = self.get_search_history(limit=10000)
        if not rows:
            return ""

        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=[
                "id", "created_at",
                "first_name", "last_name", "birth_date", "nationality", "profession",
                "gda_decision", "gda_score", "gda_match_name", "gda_fondement", "gda_motifs",
                "ppe_detected", "ppe_keywords",
                "nationality_risk_level", "nationality_risk_label", "nationality_risk_source",
                "final_decision", "decision_reason", "outcome", "operator_note",
            ],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)
        return output.getvalue()

    def update_search(self, search_id: int, updates: Dict[str, Any]):
        """
        Met à jour un enregistrement d'historique existant (étapes PPE, outcome final).
        Seules les clés présentes dans updates sont modifiées.
        """
        if not search_id or not updates:
            return

        allowed = {
            "profession", "ppe_detected", "ppe_keywords",
            "final_decision", "decision_reason", "outcome", "operator_note",
        }
        fields = {k: v for k, v in updates.items() if k in allowed}
        if not fields:
            return

        conn = self.get_connection()
        cursor = conn.cursor()
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [search_id]
        cursor.execute(f"UPDATE search_history SET {set_clause} WHERE id = ?", values)
        conn.commit()
        conn.close()

    def create_client(self, client_data: Dict[str, Any]) -> int:
        """Crée un nouveau client"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO clients (first_name, last_name, birth_date, nationality, profession, email, phone_number)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            client_data.get("first_name"),
            client_data.get("last_name"),
            client_data.get("birth_date"),
            client_data.get("nationality"),
            client_data.get("profession"),
            client_data.get("email"),
            client_data.get("phone_number")
        ))

        client_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return client_id

    def get_client(self, client_id: int) -> Optional[Dict[str, Any]]:
        """Récupère un client par son ID"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM clients WHERE id = ?", (client_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    def search_client(self, first_name: str, last_name: str, email: str) -> Optional[Dict[str, Any]]:
        """Recherche un client existant"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM clients 
            WHERE first_name = ? AND last_name = ? AND email = ?
        """, (first_name, last_name, email))

        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    def create_screening(self, client_id: int, screening_result: Dict[str, Any]) -> int:
        """Enregistre un résultat de screening"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO screenings (client_id, decision, decision_reason, gda_match, ppe_risk, screening_data)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            client_id,
            screening_result.get("decision"),
            screening_result.get("decision_reason"),
            1 if screening_result.get("gda_match") else 0,
            1 if screening_result.get("ppe_risk") else 0,
            json.dumps(screening_result)
        ))

        screening_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return screening_id

    def get_all_screenings(self, client_id: int) -> List[Dict[str, Any]]:
        """Récupère tous les screenings d'un client"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM screenings 
            WHERE client_id = ? 
            ORDER BY created_at DESC
        """, (client_id,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def create_subscription(self, client_id: int, scpi_name: str, amount: float) -> int:
        """Crée une souscription"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO subscriptions (client_id, scpi_name, amount)
            VALUES (?, ?, ?)
        """, (client_id, scpi_name, amount))

        subscription_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return subscription_id

    def get_client_subscriptions(self, client_id: int) -> List[Dict[str, Any]]:
        """Récupère toutes les souscriptions d'un client"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM subscriptions 
            WHERE client_id = ? 
            ORDER BY created_at DESC
        """, (client_id,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def create_alert(self, client_id: int, alert_type: str, severity: str,
                     message: str, previous_status: str = None, new_status: str = None) -> int:
        """Crée une alerte"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO alerts (client_id, alert_type, severity, message, previous_status, new_status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (client_id, alert_type, severity, message, previous_status, new_status))

        alert_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return alert_id

    def get_all_alerts(self) -> List[Dict[str, Any]]:
        """Récupère toutes les alertes"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT a.*, c.first_name, c.last_name 
            FROM alerts a
            JOIN clients c ON a.client_id = c.id
            ORDER BY a.created_at DESC
        """)

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_unread_alerts(self) -> List[Dict[str, Any]]:
        """Récupère les alertes non lues"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT a.*, c.first_name, c.last_name 
            FROM alerts a
            JOIN clients c ON a.client_id = c.id
            WHERE a.is_read = 0
            ORDER BY a.created_at DESC
        """)

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def mark_alert_as_read(self, alert_id: int):
        """Marque une alerte comme lue"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE alerts 
            SET is_read = 1 
            WHERE id = ?
        """, (alert_id,))

        conn.commit()
        conn.close()

    def get_all_clients(self) -> List[Dict[str, Any]]:
        """Récupère tous les clients"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM clients ORDER BY created_at DESC")
        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_clients_with_latest_screening(self) -> List[Dict[str, Any]]:
        """Récupère tous les clients avec leur dernier screening"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 
                c.*,
                s.decision as last_decision,
                s.decision_reason as last_decision_reason,
                s.gda_match,
                s.ppe_risk as is_ppe_risk,
                s.created_at as last_screening_date
            FROM clients c
            LEFT JOIN (
                SELECT client_id, decision, decision_reason, gda_match, ppe_risk, created_at,
                       ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY created_at DESC) as rn
                FROM screenings
            ) s ON c.id = s.client_id AND s.rn = 1
            ORDER BY c.created_at DESC
        """)

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]
