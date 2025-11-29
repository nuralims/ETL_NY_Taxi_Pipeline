# <img src="image.png" alt="ny taxi" width="80" style="vertical-align:middle" /> ETL NY Taxi Pipeline

Pipeline ini menjalankan alur end-to-end untuk memuat data Yellow Taxi ke Postgres, mentransformasikannya dengan dbt, lalu mengirim hasil agregasi ke BigQuery menggunakan Airflow di dalam Docker.

## Arsitektur Singkat

- **Airflow + Docker Compose** menjalankan Postgres, Redis, scheduler, worker, webserver, serta volume bersama (`dags/`, `data/`, `dbt/`, `secrets/`).
- **Task utama DAG `ny_taxi_etl_pg_bq`**
  1. `extract_data` &mdash; mengunduh berkas Parquet ke `/opt/airflow/data/`.
  2. `extract_data_to_pg` &mdash; memecah Row Group Parquet menjadi batch dan menulis ke Postgres tabel `raw_yellow_taxi_data`.
  3. `dbt_run_staging` &mdash; menjalankan model staging `stg_yellow_tripdata` untuk menormalkan kolom.
  4. `dbt_run_marts` &mdash; membangun mart `fact_trips` (dan dimensi lain) di BigQuery.
  5. `load_data` &mdash; menarik dataset mart (`public_mart.fact_trips`) dari Postgres secara bertahap dan mengirimnya ke tabel `<dataset>.ny_taxi.fact_trips_yellow_taxi_data` memakai BigQuery client.
- **dbt Project (`dbt/`)** menyediakan model staging, dimension, dan fact. Contoh baru: `models/marts/dim_pickup_date.sql` untuk tabel kalender pickup.

## Prasyarat

1. **Docker Desktop** (≥4 CPU, 8 GB RAM disarankan) dan `docker compose` plugin.
2. **GCP Service Account** dengan akses ke BigQuery. Salin file JSON-nya ke `secrets/<file>.json` (atau nama lain) lalu pastikan path ini sama seperti yang digunakan fungsi `bigquery_connect()` (`/opt/airflow/secrets/...`).
3. **Koneksi Airflow**
   - `airflow_db` (sudah disediakan di `docker-compose.yaml`).
   - `google_cloud_default` atau gunakan env var `GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/secrets/<file>.json` pada semua service Airflow.
4. **Dependensi Python**: tambahkan ke `requirements.txt` (mis. `dbt-bigquery`, `google-cloud-bigquery`, `fastparquet`, `pyarrow`) lalu rebuild image agar tersedia di seluruh container.
5. **dbt profiles**: file `dbt/profiles.yml` harus menunjuk ke service account dan dataset BigQuery yang sama.

## Setup & Menjalankan

1. **Clone repo & buat folder volume** (jika belum)
   ```powershell
   git clone <repo-url>
   cd ETL_NY_TAXI
   mkdir data logs plugins secrets
   ```
2. **Masukkan kredensial** ke `secrets/<file>.json` (atau nama lain) dan pastikan `.env`/Airflow env memuat `GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/secrets/<file>.json`.
3. **Konfigurasi variabel lingkungan** (`.env`):
   ```bash
   AIRFLOW_UID=50000
   ```
4. **Build & jalankan stack** (akan meng-install dependency terbaru dari `requirements.txt`)
   ```powershell
   docker compose build
   ```
5. **Inisialisasi database Airflow** (jika pertama kali)
   ```powershell
   docker compose run airflow-init
   docker compose up -d
   ```
6. **Akses Airflow UI** pada `http://localhost:8080` (user/pass default `airflow`/`airflow`). Aktifkan DAG `ny_taxi_etl_pg_bq` dan trigger manual.

## Penjelasan DAG `ny_taxi_etl_pg_bq`

| Task                 | Jenis            | Ringkasan                                                                                                                                                         |
| -------------------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `extract_data`       | `BashOperator`   | Mengunduh Parquet Yellow Taxi ke volume data.                                                                                                                     |
| `extract_data_to_pg` | `PythonOperator` | Membaca Parquet per Row Group (1.000 baris/batch) lalu menulis ke `raw_yellow_taxi_data` di Postgres.                                                             |
| `dbt_run_staging`    | `BashOperator`   | `dbt run --select stg_yellow_tripdata` untuk membuat view staging di Database Postgre.                                                                            |
| `dbt_run_marts`      | `BashOperator`   | Menjalankan model marts (`fact_trips`, `dim_pickup_date`, dll).                                                                                                   |
| `load_data`          | `PythonOperator` | Membaca tabel mart `public_mart.fact_trips` dari Postgres dengan batch configurable (`op_kwargs`) dan memuat ke BigQuery menggunakan `load_table_from_dataframe`. |

> **Tips**: Jika ukuran data besar, gunakan `batch_size` yang lebih besar (contoh 50.000) dan batasi query (`WHERE pickup_date >= ...`, `LIMIT ...`) agar worker tidak keluar dengan exit code `-9`.

### Menyesuaikan Query Postgres

- Set `query` pada fungsi `transfer_data_postgres_to_bigquery()` untuk hanya mengambil baris yang diperlukan. Contoh:
  ```python
  query = """
        SELECT *
        FROM public_mart.fact_trips
        WHERE pickup_datetime >= date '2025-01-01'
        ORDER BY pickup_datetime
        LIMIT 200000
  """
  ```
- Parameter `batch_size` dapat diubah lewat `op_kwargs` ketika mendefinisikan task `load_data`.

## Struktur dbt

```
dbt/
├─ dbt_project.yml
├─ profiles.yml (dipakai di container via `--profiles-dir .`)
├─ models/
│  ├─ staging/
│  │  └─ stg_yellow_tripdata.sql
│  └─ marts/
│     ├─ fact_trips.sql
│     └─ dim_pickup_date.sql
```

- Jalankan dbt lokal:
  ```powershell
  cd dbt
  dbt deps
  dbt run --profiles-dir . --select stg_yellow_tripdata fact_trips dim_pickup_date
  dbt test --profiles-dir .
  ```
- Model `stg_yellow_tripdata` membuat surrogate key `trip_id`, menormalkan tipe kolom, dan menghitung `trip_duration_minutes`.
- `fact_trips` memfilter perjalanan tidak valid dan menghitung `gross_revenue`.
- `dim_pickup_date` menyediakan atribut tanggal (tahun, bulan, weekend flag, dsb.).

## Monitoring & Troubleshooting

- **Zombie / exit code -9**: biasanya karena worker OOM atau job terlalu lama. Solusi: tingkatkan `batch_size`, batasi query, naikkan resource Docker Desktop, atau set `execution_timeout` / `retries` pada task `load_data`.
- **Konektivitas BigQuery**: pastikan file JSON terbaca dan service account punya peran `roles/bigquery.dataEditor` + `roles/bigquery.jobUser`.

## Langkah Lanjutan

- Tambahkan task untuk memvalidasi data (`dbt test`, `GreatExpectations`, dsb.).
- Otomatiskan penjadwalan (mis. `@daily` pada jam tertentu) dan tambahkan alert email/Slack pada kegagalan.
- Jika suatu saat butuh staging file, Anda dapat mengaktifkan kembali jalur Postgres → GCS → BigQuery menggunakan operator yang sudah disediakan provider GCP.

Dengan README ini, siapa pun bisa mengikuti alur dari penyiapan Docker, menjalankan DAG Airflow, sampai memahami struktur dbt untuk analitik di BigQuery. Selamat bereksperimen!
