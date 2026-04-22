def cek_rekening(bank_code, account_number, account_name):
    if not API_KEY:
        return {"error": "API key tidak ditemukan"}

    # ✅ URL BENAR sesuai dokumentasi api.co.id
    url = "https://api.co.id/v1/validation/bank"

    try:
        response = requests.get(
            url,
            headers={
                "x-api-co-id": API_KEY,
                "Accept": "application/json"
            },
            params={
                "bank_code": bank_code.lower(),
                "account_number": account_number,
                "account_name": account_name
            },
            timeout=15
        )

        # ✅ Cetak detail error untuk debug
        print(f"STATUS: {response.status_code}")
        print(f"RESPONSE: {response.text}")
        print(f"URL CALLED: {response.url}")

        if response.status_code == 401:
            return {"error": "API key tidak valid"}
        if response.status_code == 402:
            return {"error": "Saldo kredit habis"}
        if response.status_code != 200:
            return {"error": f"HTTP {response.status_code}"}

        return response.json()

    except requests.exceptions.Timeout:
        print("ERROR: Timeout")
        return {"error": "Timeout"}
    except Exception as e:
        print(f"ERROR API: {e}")
        return {"error": str(e)}
