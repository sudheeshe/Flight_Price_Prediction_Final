from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cloud_config = {
        'secure_connect_bundle': r'C:\Users\Sudheesh E\Dropbox\PC\Downloads\secure-connect-flight-price-prediction.zip'
}
auth_provider = PlainTextAuthProvider('caqhALHQBBIUIZWptJlnouhX', '9M86beObXwhCR+_L,FSbYX4ZeF_nJ39.pjmDtn8Za-N3L9P+kJwrDxhP4MPZ_a4hRy3Z2FEnxRCI_5SNIJZPm6eJhAvDFG-7F-ZeqPGiqk-BCy+xOr1mZf043J4boBhS')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
    print(row[0])
else:
    print("An error occurred.")
