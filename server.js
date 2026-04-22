const express = require("express");
const axios = require("axios");

const app = express();

const API_KEY = process.env.APICOID_API_KEY;

// endpoint yang akan dipanggil flask lo
app.get("/validation/bank", async (req, res) => {
  try {
    const { bank_code, account_number, account_name } = req.query;

    const response = await axios.get(
      "https://api.co.id/v1/validation/bank",
      {
        headers: {
          "x-api-co-id": API_KEY
        },
        params: {
          bank_code,
          account_number,
          account_name
        },
        timeout: 10000
      }
    );

    res.json(response.data);
  } catch (err) {
    console.log("ERROR API:", err.message);

    if (err.response) {
      return res.status(err.response.status).json(err.response.data);
    }

    res.status(500).json({ error: err.message });
  }
});

app.listen(3003, () => {
  console.log("API SERVICE RUNNING ON 3003 🔥");
});
