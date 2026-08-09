## Statut du projet

Projet en cours de développement.

Actuellement disponible :

- structure du dépôt ;
- documentation initiale ;
- environnement local ;
- préparation du pipeline d’ingestion.
Prochaines étapes :

- exploration des données NYC Taxi ;
- validation avec Great Expectations ;
- ingestion dans Amazon S3 ;
- transformation avec AWS Glue.


### Validation du pipeline

La transformation AWS Glue a traité **3 475 226 trajets** :

- **3 472 993** lignes validées ;
- **2 233** lignes isolées comme anomalies ;
- **0 ligne perdue** lors de la transformation.

➡️ [Voir le rapport de validation Athena](reports/athena_validation.md)