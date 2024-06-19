import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    import sqlalchemy
except ImportError:
    install("sqlalchemy")
    import sqlalchemy

try:
    import psycopg2
except ImportError:
    install("psycopg2")
    import psycopg2

print("sqlalchemy version:", sqlalchemy.__version__)
print("psycopg2 version:", psycopg2.__version__)
