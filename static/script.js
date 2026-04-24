let results = []
let total = 0
let done = 0

function onFileChange(input) {
  const f = input.files[0]
  if (!f) return
  document.getElementById('fileName').textContent = '📎 ' + f.name
  document.getElementById('fileName').style.display = 'inline-block'
  document.getElementById('btnProses').disabled = false
}

function proses() {

  const file = document.getElementById('fileInput').files[0]
  if (!file) return

  const code = document.getElementById('licenseCode').value.trim()
  if (!code) {
    alert("Masukkan kode akses dulu!")
    return
  }

  fetch('/verify-code', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ code: code })
  })
  .then(r => r.json())
  .then(res => {

    if (!res.success) {
      alert(res.message)
      return
    }

    startStream(file)

  })
}

function startStream(file) {

  const form = new FormData()
  form.append('file', file)

  const tbody = document.getElementById('tbody')
  tbody.innerHTML = ""
  results = []
  done = 0

  document.getElementById('progress-area').style.display = 'block'
  document.getElementById('progressBar').style.width = '0%'

  fetch('/stream', { method: 'POST', body: form })
  .then(res => {

    const reader = res.body.getReader()
    const decoder = new TextDecoder()
    let buf = ''

    function read() {
      reader.read().then(({done: streamDone, value}) => {
        if(streamDone) return

        buf += decoder.decode(value, {stream: true})
        const parts = buf.split("\n\n")
        buf = parts.pop()

        parts.forEach(p => {
          const raw = p.replace(/^data:\s*/, '')
          if (!raw.trim()) return

          const data = JSON.parse(raw)

          if(data.type === "start") {
            total = data.total
            document.getElementById('progressCount').textContent = `0 / ${total}`
          }

          if(data.type === "result") {

            results.push([
              data.index,
              data.nama,
              data.rekening,
              data.bank,
              data.nama_bank,
              data.hasil
            ])

            done++

            document.getElementById('progressCount').textContent = `${done} / ${total}`
            document.getElementById('progressBar').style.width =
              ((done / total) * 100) + '%'

            let badgeClass =
              data.hasil === "MATCH" ? "badge-match" :
              data.hasil === "TIDAK SAMA" ? "badge-diff" :
              "badge-invalid"

            tbody.innerHTML += `
              <tr>
                <td>${data.index}</td>
                <td>${data.nama}</td>
                <td>${data.rekening}</td>
                <td>${data.bank}</td>
                <td>${data.nama_bank}</td>
                <td><span class="badge ${badgeClass}">${data.hasil}</span></td>
              </tr>
            `
          }

          if(data.type === "done") {
            document.getElementById('progressBar').style.width = '100%'
            showToast("Proses selesai!")
          }

        })

        read()
      })
    }

    read()

  })
}

function showToast(msg) {
  const t = document.getElementById('toast')
  document.getElementById('toastMsg').textContent = msg
  t.style.display = 'block'
  setTimeout(() => {
    t.style.display = 'none'
  }, 4000)
}

function downloadTemplate() {
  window.location = '/template'
}

function download(format) {
  if (!results.length) {
    alert("Belum ada data!")
    return
  }

  fetch('/download', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      data: results,
      format: format
    })
  })
  .then(res => res.blob())
  .then(blob => {
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `hasil.${format}`
    a.click()
  })
}
