Les scripts présents dans ce répertoire nécessitent un cluster Kafka doté d'un topic "velib-stations". Ils utilisent l'API disponible à [http://developer.jcdecaux.com/](http://developer.jcdecaux.com/).

* `get-stations.py` : producer de records, dont chacune décrit l'état d'une station de vélo. Pour utiliser ce script, vous aurez besoin d'une clé d'API que vous ajouterez pour définir la variable `API_KEY`.
* `monitor-stations.py` : consumer qui va afficher dans la console les stations dont le nombre de vélos disponibles a changé.

Pour exécuter ces deux scripts, vous devrez installer le package [kafka-python](http://kafka-python.readthedocs.io/en/master/). Il est recommandé de l'installer dans un environnement virtuel :

    # Création et activation d'un environnement virtuel
    virtualenv --python=python3 ./venv
    source venv/bin/activate

    # Installation des dépendances.
    pip install kafka-python
