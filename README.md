# NYC Taxi Data Engineering Platform

Plateforme de données construite sur AWS pour ingérer, transformer, contrôler,
modéliser et analyser les données mensuelles des taxis de New York.

## Statut du projet

Projet en cours de développement.

Phase actuelle : initialisation du dépôt et préparation de l’environnement local.

## Objectifs

Ce projet a pour objectif de construire un pipeline Data Engineering :

- automatisé ;
- reproductible ;
- déployé avec Terraform ;
- contrôlé par des tests de qualité ;
- orchestré avec Apache Airflow ;
- documenté pour être compris et reproduit ;
- adapté à un budget AWS personnel.

## Architecture cible

```text
NYC Taxi Trip Records
        |
        v
Python extraction
        |
        v
Amazon S3 - Raw
        |
        v
AWS Glue / PySpark
        |
        v
Amazon S3 - Curated
        |
        v
AWS Glue Data Catalog
        |
        v
Amazon Athena
        |
        v
dbt
        |
        v
Power BI