import atoti as tt
import pandas as pd
import numpy as np
import threading
import time

# Initialiser la session avec configuration
config = tt.SessionConfig(user_content_storage="./atoti_content")
session = tt.Session.start(config)

# Charger le fichier CSV dans un DataFrame Pandas
path = 'conso-elec-gaz-annuelle-par-naf-agregee-region.csv'
conso_elec_gaz_df = pd.read_csv(path, encoding="utf-8")

# Charger les données initiales dans une table Atoti à partir du DataFrame Pandas
conso_elec_gaz = session.read_pandas(conso_elec_gaz_df, table_name="Conso_elec_gaz")

# Créer le cube avec la table chargée
cube = session.create_cube(conso_elec_gaz)
levels = cube.levels
hierarchies = cube.hierarchies
measures = cube.measures

# Lien vers le dashboard Atoti
print(f"Session LINK => {session.link}")

# Indicateur d'arrêt pour le thread
stop_thread = threading.Event()

# Fonction de simulation de variation de la colonne "Conso"
def simulate_real_time_update():
    while not stop_thread.is_set():
        # Appliquer une variation de ±10 % aléatoirement dans le DataFrame Pandas
        conso_elec_gaz_df["conso"] = conso_elec_gaz_df["conso"] * (1 + np.random.choice([-0.1, 0.1]))
        
        # Recharger les données dans la table Atoti
        session.tables["Conso_elec_gaz"].load_pandas(conso_elec_gaz_df)
        
        # Pause de 30 secondes avant la prochaine simulation
        time.sleep(30)

# Lancer la simulation de manière asynchrone
thread = threading.Thread(target=simulate_real_time_update)
thread.start()

try:
    # Boucle principale pour maintenir le programme actif
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    # Arrêt propre du batch de simulation
    print("Arrêt du batch de simulation.")
    stop_thread.set()  # Signaler au thread de s'arrêter
    thread.join()      # Attendre la fin du thread de simulation

# Arrêter la session Atoti
session.stop()
