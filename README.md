# 🌦️ Big Data Weather Prediction Project

## 🧠 Description

Ce projet met en place un **pipeline complet Big Data** pour la prédiction de la météo au Maroc en utilisant Spark, Kafka, HBase, NiFi et Zeppelin.  
L’objectif est de **collecter, valider, stocker et prédire les températures** en batch et en streaming, avec visualisation interactive en utilisant un écosystème Big Data moderne :

✔️ Ingestion de données météo en streaming (Kafka)  
✔️ Validation des données par NiFi
✔️ Stockage dans HBase  
✔️ Prétraitement et modélisation ML avec Apache Spark  
✔️ Prédiction en temps réel avec Spark Streaming  
✔️ Validation des données avec Apache NiFi  
✔️ Visualisation interactive avec Apache Zeppelin  
✔️ Déploiement avec Docker & `docker-compose`

---

## 📦 Architecture

Source API / capteurs
        |
        v
      Kafka (topic: weather-data)
        |
        v
   +-----------------+
   | Apache NiFi:     |
   | ConsumeKafka     |
   | ValidateRecord   |
   | PutHbaseRecord   |
   +-----------------+
        |
        v
      HBase (stockage batch validé)
        ^
        |
 Notebook ML (Batch)
  - Lecture HBase
  - Prétraitement
  - Entrainement ML
  - Sélection du meilleur modèle(LinearRegression, RandomForest, GradientBoostedTrees)
  - Sauvegarde modèle (GBT)
        |
        v
 Notebook Spark Streaming
  - Lecture Kafka
  - Prétraitement
  - Application du modèle sauvegardé
  - Visualisation temps réel dans Zeppelin

