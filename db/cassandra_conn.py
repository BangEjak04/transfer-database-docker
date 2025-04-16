from cassandra.cluster import Cluster
from db.config import CASSANDRA_HOST, CASSANDRA_KEYSPACE

def get_cassandra_session():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()
    
    keyspaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    if CASSANDRA_KEYSPACE not in [row.keyspace_name for row in keyspaces]:
        session.execute(f"""
            CREATE KEYSPACE {CASSANDRA_KEYSPACE} WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }}
        """)
        print(f"🆕 Cassandra keyspace '{CASSANDRA_KEYSPACE}' dibuat.")
    
    session.set_keyspace(CASSANDRA_KEYSPACE)
    return session
