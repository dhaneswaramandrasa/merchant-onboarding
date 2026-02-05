# Merchant Onboarding ETL System

Sistem ETL (Extract, Transform, Load) untuk mengelola dan menganalisis data onboarding merchant di platform Olsera. Sistem ini terdiri dari dua komponen utama: pengambilan data acquisition dan pembuatan laporan cohort.

## 📁 Struktur Proyek

```
merchant-onboarding/
├── README.md
├── requirements.txt
├── .env
├── .gitignore
├── get_acq_data/
│   ├── get_acq_data.py          # Script utama untuk mengambil data acquisition
│   ├── get_acq_data.sh          # Shell script untuk menjalankan ETL
│   ├── credentials.json         # Credentials Google Service Account
│   ├── logs/                    # Folder untuk log files
│   └── utils/
│       ├── connect_aurora.py    # Utility koneksi ke database Aurora
│       ├── connect_gsheet.py    # Utility koneksi ke Google Sheets
│       └── logger.py            # Utility logging
└── get_cohort_report/
    ├── get_cohort_report.py     # Script utama untuk laporan cohort
    ├── get_cohort_report.sh     # Shell script untuk menjalankan laporan
    ├── credentials.json         # Credentials Google Service Account
    ├── logs/                    # Folder untuk log files
    └── utils/
        ├── connect_aurora.py    # Utility koneksi ke database Aurora
        ├── connect_gsheet.py    # Utility koneksi ke Google Sheets
        └── logger.py            # Utility logging
```

## 🚀 Fitur

### Get Acquisition Data (`get_acq_data`)
- Mengambil data merchant yang baru melakukan pembayaran dari database Aurora
- Memfilter merchant baru (bukan renewal)
- Menulis data ke Google Sheets untuk tracking onboarding
- Menggunakan rentang tanggal 3 bulan terakhir dari tanggal pembayaran

### Cohort Report (`get_cohort_report`)
- Membuat analisis cohort untuk merchant yang baru di-onboard
- Mengukur aktivitas transaksi dalam 7 hari terakhir
- Mengkategorikan merchant sebagai Active/Non-active berdasarkan transaksi
- Menghasilkan summary onboarding per bulan
- Menghitung persentase onboarding dan cohort merchant

## 📋 Prerequisites

- Python 3.8+
- Akses ke database Aurora MySQL
- Google Service Account dengan akses ke Google Sheets API
- Environment variables yang dikonfigurasi

## ⚙️ Setup

1. **Clone repository:**
   ```bash
   git clone https://github.com/Olsera/merchant-onboarding.git
   cd merchant-onboarding
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Konfigurasi environment variables:**

   Buat file `.env` di root directory dengan variabel berikut:
   ```env
   # Database Configuration
   DB_HOST=your-aurora-host
   DB_PORT=3306
   DB_USER=your-db-user
   DB_PASS=your-db-password
   DB_NAME=your-database-name

   # Google Sheets Configuration
   SPREADSHEET_ID=your-spreadsheet-id
   SPREADSHEET_ID_SOURCE=your-source-spreadsheet-id
   ```

4. **Setup Google Service Account:**
   - Copy credentials JSON dari Google Cloud Console
   - Simpan sebagai `credentials.json` di setiap folder (`get_acq_data/` dan `get_cohort_report/`)

## 🔧 Cara Menjalankan

### Menjalankan Get Acquisition Data
```bash
# Menggunakan Python script langsung
cd get_acq_data
python get_acq_data.py --end_date 2024-01-31

# Atau menggunakan shell script
./get_acq_data.sh
```

### Menjalankan Cohort Report
```bash
# Menggunakan Python script langsung
cd get_cohort_report
python get_cohort_report.py

# Atau menggunakan shell script
./get_cohort_report.sh
```

## 📊 Output

### Get Acquisition Data
Menghasilkan data merchant baru dengan kolom:
- Periode
- No Invoice
- No Billing
- Store ID
- Data Channel
- URL ID
- Payment Date
- Note
- Expire Date
- Activated Date
- Number of Product
- On Board Date

### Cohort Report
Menghasilkan multiple sheet di Google Sheets:
1. **Raw Data**: Data onboarding lengkap
2. **Transaction Data**: Data transaksi harian per merchant
3. **Summary**: Analisis cohort dengan status aktif/non-aktif
4. **Monthly Summary**: Agregat per bulan dengan metrics:
   - Store Acquisition
   - Total Onboard
   - Regular/Late Onboard
   - Active/Non-active Merchant
   - Persentase Onboard dan Cohort

## 🔍 Logging

Semua aktivitas dicatat dalam folder `logs/` dengan format:
- Timestamp
- Level log (INFO, ERROR, etc.)
- Pesan log

## 🛠️ Dependencies

- `pandas`: Data manipulation
- `pymysql`: MySQL database connection
- `python-dotenv`: Environment variable management
- `python-dateutil`: Date utilities
- `google-api-python-client`: Google APIs client
- `google-auth-httplib2`: Google auth HTTP library
- `google-auth-oauthlib`: Google auth OAuth library

## 📝 Catatan

- Pastikan credentials Google Service Account memiliki akses edit ke spreadsheet target
- Script menggunakan timezone lokal server
- Data transaksi hanya menghitung yang `is_paid = 1`
- Merchant dikategorikan non-active jika memiliki ≤4 hari dengan transaksi rendah dalam seminggu

## 🤝 Contributing

1. Fork repository
2. Buat feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add some AmazingFeature'`)
4. Push ke branch (`git push origin feature/AmazingFeature`)
5. Buat Pull Request

## 📞 Support

Untuk pertanyaan atau issues, silakan buat issue di repository ini.