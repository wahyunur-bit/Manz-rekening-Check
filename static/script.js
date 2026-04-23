let results = [];

function upload() {
    const file = document.getElementById("file").files[0];

    if (!file) {
        alert("Pilih file dulu");
        return;
    }

    document.getElementById("table").innerHTML = "";
    results = [];

    const formData = new FormData();
    formData.append("file", file);

    fetch("/stream", {
        method: "POST",
        body: formData
    }).then(response => {
        const reader = response.body.getReader();
        const decoder = new TextDecoder();

        function read() {
            reader.read().then(({ done, value }) => {
                if (done) return;

                const chunk = decoder.decode(value);
                const lines = chunk.split("\n\n");

                lines.forEach(line => {
                    if (line.startsWith("data:")) {
                        const data = JSON.parse(line.replace("data: ", ""));

                        if (data.type === "result") {
                            results.push(data);
                            addRow(data);
                        }
                    }
                });

                read();
            });
        }

        read();
    });
}

function addRow(d) {
    document.getElementById("table").insertAdjacentHTML("beforeend", `
        <tr>
            <td>${d.index}</td>
            <td>${d.nama}</td>
            <td>${d.rekening}</td>
            <td>${d.bank}</td>
            <td>${d.nama_bank}</td>
            <td>${d.hasil}</td>
        </tr>
    `);
}
