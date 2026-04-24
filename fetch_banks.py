"""
Fetch daftar semua bank yang didukung dari API dan simpan ke Excel.
Jalankan: python fetch_banks.py
Pastikan env variable APICOID_API_KEY sudah di-set.
"""
import requests
import pandas as pd
import os

API_KEY = os.getenv("APICOID_API_KEY")
URL = "https://use.api.co.id/validation/bank/available"

if not API_KEY:
    print("ERROR: Set env variable APICOID_API_KEY terlebih dahulu!")
    print("  Windows CMD:   set APICOID_API_KEY=your_key_here")
    print("  PowerShell:    $env:APICOID_API_KEY='your_key_here'")
    exit(1)

print("Fetching daftar bank dari API...")
res = requests.get(URL, headers={"x-api-co-id": API_KEY}, timeout=15)

if res.status_code != 200:
    print(f"HTTP Error {res.status_code}: {res.text[:500]}")
    exit(1)

data = res.json()
if not data.get("is_success"):
    print(f"API Error: {data}")
    exit(1)

banks = data["data"]["banks"]
total = data["data"].get("total", len(banks))

print(f"Total bank ditemukan: {total}")

# Buat DataFrame
df = pd.DataFrame(banks)
df.index = df.index + 1
df.index.name = "No"
df.columns = ["Nama Bank", "Kode Bank"]

# Tambah kolom short code (tanpa prefix bank_)
df["Kode Singkat"] = df["Kode Bank"].str.replace("bank_", "", n=1)

# Simpan ke Excel
output = "daftar_bank.xlsx"
df.to_excel(output)
print(f"Berhasil disimpan ke: {output}")
print(f"\nContoh 10 bank pertama:")
print(df.head(10).to_string())
