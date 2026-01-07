from enum import Enum

class RepositoryType(str, Enum):
    Neo4j = "neo4j"

RepositoryList = list(RepositoryType)

