import os
from dotenv import load_dotenv

load_dotenv()
print('URL present:', bool(os.getenv('SUPABASE_URL')))
print('KEY present:', bool(os.getenv('SUPABASE_SERVICE_KEY')))
print('URL value:', os.getenv('SUPABASE_URL'))
