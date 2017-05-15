## Velos

![schéma topologie](https://raw.githubusercontent.com/oc-courses/gerez-des-flux-de-donnees-temps-reel/master/velos/topology.png)

Compilez et exécutez la topologie en local avec :

    storm jar ./target/velos-1.0-SNAPSHOT.jar velos.App

Pour la soumettre à un cluster distant, utilisez l'option `remote` :

    storm jar ./target/velos-1.0-SNAPSHOT.jar velos.App remote

Notez que cette topologie requiert un cluster Kafka muni d'un topic "velib-stations" pour fonctionner.
