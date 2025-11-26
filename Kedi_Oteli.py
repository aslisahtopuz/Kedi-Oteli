import psycopg2
import os
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import gspread
from dateutil import parser
import traceback

load_dotenv("/Users/aslisah/Desktop/Kedi-Oteli/.env")

print("CWD:", os.getcwd())
print("GS_SERVICE_JSON:", os.getenv("GS_SERVICE_JSON"))
print("SERVICE_JSON_PATH:", os.getenv("SERVICE_JSON_PATH"))

PG = dict(
    host=os.getenv("DB_HOST"),
    port= os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    sslmode="require"
)

conn = psycopg2.connect(**PG)
conn.autocommit = False
print("DB connected.")

#Where am i connected
with conn.cursor() as cur:
    cur.execute("select current_database(), current_schema(), current_user;")
    print("Connected to:", cur.fetchone())



SERVICE_JSON = os.getenv("SERVICE_JSON_PATH")
if not SERVICE_JSON:
    raise ValueError("SERVICE_JSON_PATH is missing in environment!")
SPREADSHEET_KEY = os.getenv("SPREADSHEET_KEY")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME")


scope = ["https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]

creds = Credentials.from_service_account_file(SERVICE_JSON, scopes=scope)
gc = gspread.authorize(creds)
ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(WORKSHEET_NAME)
print("Google Sheets connection succesful")


COL = {
    "owner_name":  "Evcil Hayvan Sahibi Ad-Soyad",
    "owner_phone": "Evcil Hayvan Sahibi Cep Numara",
    "owner_addr":  "Evcil Hayvan Sahibi Adres",

    "cat_name":    "Evcil Hayvan Ad",
    "cat_age":     "Evcil Hayvan Yaş Bilgisi",
    "cat_sex":     "Evcil Hayvan Cinsiyet",
    "cat_breed":   "Evcil Hayvan Cins",
    "cat_allergy": "Alerji / Diyet",
    "chip":        "Evcil Hayvan Çip No.",
    "neuter":      "Kısır mı?",
    "taxi":        "Pet Taksi Hizmeti Alındı mı?",
    "room_type":   "Oda Tipi",
    "check_in":    "Check-in",
    "check_out":   "Check-out",

    "in_ex_date":  "İç-Dış Parazit Aşısı Tarihi",
    "karma_date":  "Karma Aşı Tarihi",
    "vacc_info":   "Aşı Bilgisi",

    "price_daily":   "Günlük Fiyat",
    "price_monthly": "Aylık Fiyat",
    "price_total":   "Toplam Fiyat",
    "notes":         "Notlar",
}


def G(r, key, default=None):
    return r.get(COL[key], default)

def d(v):
    if v in (None, "", "None"): return None
    try:
        return parser.parse(str(v), dayfirst=True).date()
    except:
        return None

def num(x):
    if x in (None, "", "None"): return None
    try:
        return float(str(x).replace(" ", "").replace(".", "").replace(",", "."))
    except Exception:
        return None

def norm_sex(s):
    s = str(s or "").strip().lower()
    if s.startswith("e"): return "male"
    if s.startswith("d"): return "female"
    return "unknown"


headers = ws.row_values(1)
if "import_status" not in headers:
    ws.update_cell(1, len(headers)+1, "import_status")
    headers = ws.row_values(1)
if "import_error" not in headers:
    ws.update_cell(1, len(headers)+1, "import_error")
    headers = ws.row_values(1)

col_status = headers.index("import_status") + 1
col_error  = headers.index("import_error")  + 1



with conn.cursor() as cur:
    cur.execute("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='cats' AND column_name='owner_id'
        """)
    CATS_HAS_OWNER = cur.fetchone() is not None


with conn.cursor() as cur:
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_owners_name_phone
                   ON public.owners(owner_name, owner_phone);""")
    # Kedide tekrar olmasın
    if CATS_HAS_OWNER:
        cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_cats_owner_name
                       ON public.cats(owner_id, cat_name);""")
conn.commit()


rows = ws.get_all_records(value_render_option='FORMATTED_VALUE')
ok = err = 0


for i, r in enumerate(rows, start=2):
    if r.get("import_status") == "Done":
        continue

    try:
        with conn.cursor() as cur:
            # OWNERS 
            cur.execute("""
                INSERT INTO public.owners(owner_name, owner_phone, owner_addr)
                VALUES (%s, %s, %s)
                ON CONFLICT (owner_name, owner_phone) DO UPDATE SET
                    owner_addr = EXCLUDED.owner_addr
                RETURNING owner_id;
            """, (G(r,"owner_name"), str(G(r,"owner_phone","")), G(r,"owner_addr")))
            
            owner_id = cur.fetchone()[0]
           

            # CATS
            if CATS_HAS_OWNER:
                cur.execute("""SELECT cat_id FROM public.cats
                               WHERE owner_id=%s AND cat_name=%s""",
                            (owner_id, G(r,"cat_name")))
            else:
                cur.execute("""SELECT cat_id FROM public.cats
                               WHERE cat_name=%s""",
                            (G(r,"cat_name"),))
            row2 = cur.fetchone()

            if row2:
                cat_id = row2[0]
                
                cur.execute("""
                    UPDATE public.cats
                    SET cat_age=%s, cat_sex=%s, cat_breed=%s,
                        cat_allergy=%s, chip=%s, neuter=%s
                    WHERE cat_id=%s
                """, (G(r,"cat_age"),
                      norm_sex(G(r,"cat_sex")),
                      G(r,"cat_breed"),
                      G(r,"cat_allergy"),
                      str(G(r,"chip","")),
                      G(r,"neuter"),
                      cat_id))
            else:
                if CATS_HAS_OWNER:
                    cur.execute("""
                        INSERT INTO public.cats(
                            owner_id, cat_name, cat_age, cat_sex, cat_breed, cat_allergy, chip, neuter
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        RETURNING cat_id;
                    """, (owner_id, G(r,"cat_name"), G(r,"cat_age"),
                          norm_sex(G(r,"cat_sex")), G(r,"cat_breed"),
                          G(r,"cat_allergy"), str(G(r,"chip","")), G(r,"neuter")))
                else:
                    cur.execute("""
                        INSERT INTO public.cats(
                            cat_name, cat_age, cat_sex, cat_breed, cat_allergy, chip, neuter
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        RETURNING cat_id;
                    """, (G(r,"cat_name"), G(r,"cat_age"),
                          norm_sex(G(r,"cat_sex")), G(r,"cat_breed"),
                          G(r,"cat_allergy"), str(G(r,"chip","")), G(r,"neuter")))
                cat_id = cur.fetchone()[0]

            # BOOKINGS 
            cur.execute("""
                INSERT INTO public.bookings(
                    cat_id, check_in, check_out,
                    price_daily, price_monthly, price_total,
                    notes, room_type
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING booking_id;
            """, ( cat_id, d(G(r,"check_in")), d(G(r,"check_out")),
                  num(G(r,"price_daily")), num(G(r,"price_monthly")), num(G(r,"price_total")),
                  G(r,"notes"), G(r,"room_type")
                 ))
            booking_id = cur.fetchone()[0]

            # VACCINATIONS 
            if G(r,"in_ex_date") or G(r,"karma_date") or G(r,"vacc_info"):
                cur.execute("""
                    INSERT INTO public.vaccinations(cat_id, in_ex_date, karma_date, vacc_info)
                    VALUES (%s,%s,%s,%s)
                """, (cat_id, d(G(r,"in_ex_date")), d(G(r,"karma_date")), G(r,"vacc_info")))

            # SERVICES 
            taxi_val = G(r, "taxi")
            if taxi_val not in (None, ""):
                cur.execute("""
                    INSERT INTO public.services (taxi, booking_id)
                    VALUES (%s, %s)
                """, (taxi_val, booking_id))


        
        conn.commit()
        ws.update_cell(i, col_status, "Done")
        ws.update_cell(i, col_error, "")
        ok += 1


       

    except Exception as e:
        conn.rollback()
        ws.update_cell(i, col_status, "Error")
        ws.update_cell(i, col_error,  traceback.format_exc())  
        err += 1


print(f"Success: {ok}, Error: {err}")

