import os
from dotenv import load_dotenv
load_dotenv()

xml_user = os.getenv("XML_USER")
xml_key = os.getenv("XML_KEY")
google_api_key = os.getenv("GOOGLE_API_KEY")
DATABASE_URL = "sqlite+aiosqlite:///database.sqlite"
