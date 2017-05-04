## Analytics

![schéma topologie](https://raw.githubusercontent.com/oc-courses/gerez-des-flux-de-donnees-temps-reel/master/analytics/topology.png)

Compilez et exécutez la topologie en local avec :

    storm jar ./target/analytics-1.0-SNAPSHOT.jar analytics.App

Pour la soumettre à un cluster distant, utilisez l'option `remote` :

    storm jar ./target/analytics-1.0-SNAPSHOT.jar analytics.App remote

Notez que cette topologie comporte (exprès) des erreurs de traitement de tuples pour illustrer la gestion des exceptions.
