# wsgi.py
import sys
import os

# Add your project directory to Python's path
project_home = os.path.dirname(os.path.abspath(__file__))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

# Import the 'app' object from your app.py file (commonly aliased as 'application')
from algo_server import app as application
