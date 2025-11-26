#!/usr/bin/env python
# coding: utf-8

# In[13]:


# --- Ayarlar / bağlantılar ---
import psycopg2
from google.oauth2.service_account import Credentials
import gspread
from dateutil import parser
import traceback


PG = dict(
    host="aws-1-ap-southeast-2.pooler.supabase.com",
    port=5432,
    dbname="postgres",
    user="postgres.dyyoxphwtisivzivzzvb",
    password="HekaOzgur06",
    sslmode="require"
    
)

conn = psycopg2.connect(**PG)
conn.autocommit = False
print("DB bağlandı")

try:
    test_conn = psycopg2.connect(**PG)
    print("✅ Supabase bağlantısı başarılı!")
    test_conn.close()
except Exception as e:
    print("❌ Bağlantı hatası:", e)


SERVICE_JSON    = "kedioteli-1ad8252b41ac.json"
SPREADSHEET_KEY = "1Kt0-4or7zy_8VviyP_nmdVmnBVpxdxADy-ihUZmK2J0"
WORKSHEET_NAME  = "Form Yanıtları 1"

scope = ["https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(SERVICE_JSON, scopes=scope)
gc = gspread.authorize(creds)
ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(WORKSHEET_NAME)
print("Google Sheets bağlantısı kuruldu")

# --- Başlık eşlemesi (Sheet sütun adları birebir olsun) ---
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


# --- yardımcılar ---
def G(r, key, default=None):
    return r.get(COL[key], default)

def d(v):
    if v in (None, "", "None"): return None
    try:
        return parser.parse(str(v), dayfirst=True).date()
    except Exception:
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

# --- Sheet kolonları (status/error yoksa ekle) ---
headers = ws.row_values(1)
if "import_status" not in headers:
    ws.update_cell(1, len(headers)+1, "import_status")
    headers = ws.row_values(1)
if "import_error" not in headers:
    ws.update_cell(1, len(headers)+1, "import_error")
    headers = ws.row_values(1)

col_status = headers.index("import_status") + 1
col_error  = headers.index("import_error")  + 1
print("status col:", col_status, "| error col:", col_error)

# --- cats.owner_id var mı? otomatik algıla ---
with conn.cursor() as cur:
    cur.execute("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='cats' AND column_name='owner_id'
        """)
    CATS_HAS_OWNER = cur.fetchone() is not None

# --- Opsiyonel: gerekli indexler (idempotent) ---
with conn.cursor() as cur:
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_owners_name_phone
                   ON public.owners(owner_name, owner_phone);""")
    # Kedide tekrarın önüne geç (owner_id varsa bunu kullan)
    if CATS_HAS_OWNER:
        cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_cats_owner_name
                       ON public.cats(owner_id, cat_name);""")
conn.commit()

# --- Satırları işle ---
rows = ws.get_all_records(value_render_option='FORMATTED_VALUE')
ok = err = 0


for i, r in enumerate(rows, start=2):
    st = str(r.get("import_status", "")).strip()
    if st == "Done":   # sadece Done olanları atla
        continue

    try:
        with conn.cursor() as cur:
            # --- OWNERS (upsert) ---
            cur.execute("""
                INSERT INTO public.owners(owner_name, owner_phone, owner_addr)
                VALUES (%s, %s, %s)
                ON CONFLICT (owner_name, owner_phone) DO UPDATE SET
                    owner_addr = EXCLUDED.owner_addr
                RETURNING owner_id;
            """, (G(r,"owner_name"), str(G(r,"owner_phone","")), G(r,"owner_addr")))
            if cur.rowcount:
                owner_id = cur.fetchone()[0]
            else:
                cur.execute("""SELECT owner_id FROM public.owners
                               WHERE owner_name=%s AND owner_phone=%s""",
                            (G(r,"owner_name"), str(G(r,"owner_phone",""))))
                owner_id = cur.fetchone()[0]

            # --- CATS (var mı bak; yoksa ekle) ---
            if CATS_HAS_OWNER:
                cur.execute("""SELECT cat_id FROM public.cats
                               WHERE owner_id=%s AND cat_name=%s""",
                            (owner_id, G(r,"cat_name")))
            else:
                cur.execute("""SELECT cat_id FROM public.cats
                               WHERE cat_name=%s""",
                            (G(r,"cat_name"),))
            row = cur.fetchone()

            if row:
                cat_id = row[0]
                # İstersen güncelle:
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

            # --- BOOKINGS (mevcut kolonlara göre) ---
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

            # --- VACCINATIONS (sadece veri varsa ekle) ---
            if G(r,"in_ex_date") or G(r,"karma_date") or G(r,"vacc_info"):
                cur.execute("""
                    INSERT INTO public.vaccinations(cat_id, in_ex_date, karma_date, vacc_info)
                    VALUES (%s,%s,%s,%s)
                """, (cat_id, d(G(r,"in_ex_date")), d(G(r,"karma_date")), G(r,"vacc_info")))

            # --- SERVICES (Pet Taksi vs.) ---
            taxi_val = G(r, "taxi")
            if taxi_val:
                cur.execute("""
                    INSERT INTO public.services (taxi, booking_id)
                    VALUES (%s, %s)
                """, (taxi_val, booking_id))


        
        conn.commit()
        ws.update_cell(i, col_status, "Done")
        ws.update_cell(i, col_error, "")
        ok += 1

    #"except Exception as e:
        #"conn.rollback()
        #"print(f""[ROW {i}] ERROR: {e}"")
        #"ws.update_cell(i, col_status, ""Error"")
        #"ws.update_cell(i, col_error, str(e))
        #"err += 1"

       

    except Exception as e:
        conn.rollback()
        tb = traceback.format_exc()
        ws.update_cell(i, col_status, "Error")
        ws.update_cell(i, col_error,  f"{e}\n{tb}")  # <- stack trace'i de yaz
        err += 1


print(f"Tamamlandı ➜ Başarılı: {ok}, Hatalı: {err}")




# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




