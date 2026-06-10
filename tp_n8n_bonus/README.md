# Chatbot Gestionnaire de tâches — n8n

Chatbot Telegram qui permet de gérer une liste de tâches personnelles.
L'utilisateur envoie une commande, n8n l'interprète, lit ou écrit dans une base
PostgreSQL, puis répond automatiquement sur Telegram.

---

## Scénario

Un utilisateur veut gérer ses tâches directement depuis Telegram, sans application
dédiée. Il dispose de trois commandes :

| Commande         | Effet                                  | Exemple              |
|------------------|----------------------------------------|----------------------|
| `/add <texte>`   | Ajoute une tâche                       | `/add Acheter du pain` |
| `/list`          | Affiche les tâches en cours            | `/list`              |
| `/done <id>`     | Marque une tâche comme terminée        | `/done 3`            |

Toute autre commande déclenche un message d'aide listant les commandes disponibles.

---

## Interactions démontrées

Le workflow montre une vraie chaîne d'interaction entre quatre acteurs :

```
Utilisateur  ⇄  Telegram  ⇄  n8n  ⇄  PostgreSQL
```

- **Utilisateur** : envoie un message texte
- **Telegram** : service externe de messagerie (entrée et sortie)
- **n8n** : orchestre, interprète la commande, décide quoi faire
- **PostgreSQL** : base de données qui stocke les tâches

---

## Schéma du workflow

```
Telegram Trigger
      ↓
Analyser la commande      (extrait commande + argument)
      ↓
Router la commande        (Switch selon /add, /list, /done, ou autre)
      ├── /add  → Ajouter en base    → Confirmer l'ajout    ─┐
      ├── /list → Lister les tâches  → Formater la liste    ─┤
      ├── /done → Terminer une tâche → Confirmer la clôture ─┤
      └── autre → Aide                                      ─┤
                                                             ↓
                                              Répondre sur Telegram
```

---

## Description des nœuds

| Nœud                     | Type             | Rôle                                                       |
|--------------------------|------------------|------------------------------------------------------------|
| Telegram Trigger         | Telegram Trigger | Déclenche le workflow à chaque message reçu                |
| Analyser la commande     | Code             | Extrait la commande (`/add`...) et l'argument du message   |
| Router la commande       | Switch           | Aiguille vers la bonne branche selon la commande           |
| Ajouter en base          | Postgres         | `INSERT` d'une nouvelle tâche                              |
| Lister les tâches        | Postgres         | `SELECT` des tâches non terminées                          |
| Terminer une tâche       | Postgres         | `UPDATE` pour passer une tâche à terminée                  |
| Confirmer l'ajout        | Code             | Prépare le message de confirmation d'ajout                 |
| Formater la liste        | Code             | Met en forme la liste des tâches                           |
| Confirmer la clôture     | Code             | Prépare la confirmation (ou l'échec) de clôture            |
| Aide                     | Code             | Construit le message d'aide                                |
| Répondre sur Telegram    | Telegram         | Envoie la réponse finale à l'utilisateur                   |

---

## Services utilisés

- **Telegram** — messagerie (réception et envoi des messages)
- **PostgreSQL** — stockage persistant des tâches
- **n8n** — orchestration du workflow

---

## Table PostgreSQL

```sql
CREATE TABLE tasks (
    id          SERIAL      PRIMARY KEY,
    chat_id     BIGINT      NOT NULL,    -- identifie l'utilisateur Telegram
    label       TEXT        NOT NULL,    -- texte de la tâche
    is_done     BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
    done_at     TIMESTAMP                -- rempli quand la tâche est terminée
);
```

Le champ `chat_id` permet à chaque utilisateur de ne voir que ses propres tâches :
toutes les requêtes filtrent dessus.

Le script complet est dans `init_db.sql`.

---

## Mise en place

### 1. Créer le bot Telegram

1. Sur Telegram, ouvrir une conversation avec **@BotFather**
2. Envoyer `/newbot`, suivre les instructions
3. BotFather donne un **token** (ex. `123456:ABC-DEF...`) — le garder de côté

### 2. Préparer la base PostgreSQL

Exécuter le script `init_db.sql` sur ta base :
```bash
psql -U <user> -d <database> -f init_db.sql
```

### 3. Importer le workflow dans n8n

1. Ouvrir n8n
2. Menu **Workflows → Import from File**
3. Sélectionner `workflow_chatbot_taches.json`

### 4. Configurer les credentials dans n8n

Le workflow référence deux credentials à créer (Menu **Credentials → New**) :

**Telegram account** (type *Telegram API*)
| Champ        | Valeur                          |
|--------------|---------------------------------|
| Access Token | le token donné par BotFather    |

**Postgres tasks** (type *Postgres*)
| Champ    | Valeur                  |
|----------|-------------------------|
| Host     | adresse de ta base      |
| Database | nom de la base          |
| User     | utilisateur PostgreSQL  |
| Password | mot de passe            |
| Port     | `5432`                  |

Une fois créés, vérifier que chaque nœud Telegram/Postgres pointe bien sur le bon
credential (ils se rattachent automatiquement s'ils portent le même nom).

### 5. Activer le workflow

Basculer le workflow sur **Active** (interrupteur en haut à droite).
Le Telegram Trigger met alors en place un webhook qui écoute les messages.

---

## Démonstration

Dans la conversation Telegram avec ton bot :

```
Toi  : /add Réviser le TP n8n
Bot  : Tâche ajoutée : Réviser le TP n8n

Toi  : /add Acheter du pain
Bot  : Tâche ajoutée : Acheter du pain

Toi  : /list
Bot  : Vos tâches en cours :
       #1 — Réviser le TP n8n
       #2 — Acheter du pain

Toi  : /done 1
Bot  : Tâche terminée : Réviser le TP n8n

Toi  : /list
Bot  : Vos tâches en cours :
       #2 — Acheter du pain

Toi  : bonjour
Bot  : Commandes disponibles :
       /add <texte>  — ajouter une tâche
       /list         — voir les tâches en cours
       /done <id>    — marquer une tâche comme terminée
```

---

## Structure du projet

```
tp_n8n/
├── workflow_chatbot_taches.json   # Workflow n8n importable
├── init_db.sql                    # Création de la table tasks
└── README.md
```

---

## Limites

- Pas de gestion des accès concurrents (suffisant pour un usage personnel).
- `/done` attend un identifiant numérique valide ; un argument non numérique
  renverra simplement « aucune tâche trouvée ».
- Le bot répond à tout utilisateur qui le contacte ; aucune restriction d'accès
  n'est mise en place.