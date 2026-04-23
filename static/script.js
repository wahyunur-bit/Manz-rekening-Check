let results = []

function start() {
    const file = document.getElementById('file').files[0]
    const form = new FormData()
    form.append('file', file)

    const tbody = document.getElementById('tbody')
    tbody.innerHTML = ""
    results = []

    fetch('/stream', { method: 'POST', body: form })
    .then(res => {
        const reader = res.body.getReader()
        const decoder = new TextDecoder()

        function read() {
            reader.read().then(({done, value}) => {
                if(done) return
                const chunk = decoder.decode(value)
                chunk.split("\n\n").forEach(line => {
                    if(line.startsWith("data:")) {
                        const data = JSON.parse(line.replace("data: ", ""))

                        if(data.type === "result") {
                            results.push(data)

                            tbody.innerHTML += `
                                <tr>
                                    <td>${data.index}</td>
                                    <td>${data.nama}</td>
                                    <td>${data.rekening}</td>
                                    <td>${data.bank}</td>
                                    <td>${data.nama_bank}</td>
                                    <td>${data.hasil}</td>
                                </tr>
                            `
                        }
                    }
                })
                read()
            })
        }

        read()
    })
}

function download(format) {
    fetch('/download', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            data: results.map(r => [
                r.index, r.nama, r.rekening, r.bank, r.nama_bank, r.hasil
            ]),
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
