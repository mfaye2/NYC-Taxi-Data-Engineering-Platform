# Great Expectations

Ce dossier contiendra les règles et les workflows de validation des données du
projet.

Les contrôles porteront notamment sur :

- la présence des colonnes attendues ;
- les dates de départ et d’arrivée ;
- la cohérence de la durée des trajets ;
- les montants négatifs ;
- les distances anormales ;
- les identifiants de zones ;
- les doublons ;
- les valeurs nulles ;
- les variations inhabituelles du volume mensuel.

Great Expectations sera exécuté localement et intégré au pipeline Apache Airflow.