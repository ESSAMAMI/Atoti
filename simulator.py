import atoti as tt
import pandas as pd
import numpy as np
import threading
import time
import logging

# Configurer le logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AtotiSimulation:
    def __init__(self, path: str, storage_path: str = "./atoti_content"):
        self.path = path
        self.config = tt.SessionConfig(user_content_storage=storage_path)
        self.session = None
        self.cube = None
        self.conso_elec_gaz_df = None
        self.stop_thread = threading.Event()
        logging.info("AtotiSimulation instance created.")

    def start_session(self):
        """Initialise la session Atoti, charge les données et configure le cube."""
        self.session = tt.Session.start(self.config)
        logging.info("Atoti session started.")

        # Charger les données initiales
        self.conso_elec_gaz_df = pd.read_csv(self.path, encoding="utf-8")
        conso_elec_gaz = self.session.read_pandas(self.conso_elec_gaz_df, table_name="Conso_elec_gaz")
        
        # Créer le cube avec la table chargée
        self.cube = self.session.create_cube(conso_elec_gaz)
        logging.info("Data loaded and cube created.")

        # Lien vers le dashboard Atoti
        logging.info(f"Session LINK => {self.session.link}")

    def simulate_real_time_update(self):
        """Simule des mises à jour des données en temps réel pour 3 régions choisies aléatoirement."""
        while not self.stop_thread.is_set():
            try:
                # Sélectionner 3 régions de façon aléatoire
                regions_sample = self.conso_elec_gaz_df["libelle_region"].dropna().unique()
                selected_regions = np.random.choice(regions_sample, size=3, replace=False)
                
                # Appliquer la variation de ±10 % uniquement aux lignes des régions sélectionnées
                mask = self.conso_elec_gaz_df["libelle_region"].isin(selected_regions)
                variation = np.random.choice([-0.1, 0.1])
                self.conso_elec_gaz_df.loc[mask, "conso"] *= (1 + variation)
                
                # Recharger les données dans la table Atoti
                self.session.tables["Conso_elec_gaz"].load_pandas(self.conso_elec_gaz_df)
                logging.info(f"Applied a {variation*100}% variation to regions: {selected_regions.tolist()}")

                # Pause de 30 secondes avant la prochaine simulation
                time.sleep(30)
            except Exception as e:
                logging.error(f"Error in simulate_real_time_update: {e}")

    def start_simulation(self):
        """Démarre la simulation en mode asynchrone."""
        self.thread = threading.Thread(target=self.simulate_real_time_update)
        self.thread.start()
        logging.info("Simulation started.")

    def stop_simulation(self):
        """Arrête proprement la simulation et la session Atoti."""
        logging.info("Stopping simulation...")
        self.stop_thread.set()
        self.thread.join(timeout=5)  # Timeout pour éviter un blocage
        self.session.stop()
        logging.info("Simulation and Atoti session stopped.")

# Utilisation de la classe AtotiSimulation
if __name__ == "__main__":
    # Initialiser et démarrer la simulation
    simulation = AtotiSimulation(path='conso-elec-gaz-annuelle-par-naf-agregee-region.csv')
    simulation.start_session()
    simulation.start_simulation()

    try:
        # Boucle principale pour maintenir le programme actif
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Arrêt propre de la simulation en cas d'interruption
        logging.info("KeyboardInterrupt received. Stopping simulation...")
    finally:
        simulation.stop_simulation()
