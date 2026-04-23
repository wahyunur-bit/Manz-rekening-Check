let results = [];

function upload() {
    const file = document.getElementById("file").files[0];

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

function addRow(data) {
    const table = document.getElementById("table");

    table.insertAdjacentHTML("beforeend", `
        <tr>
            <td>${data.index}</td>
            <td>${data.nama}</td>
            <td>${data.rekening}</td>
            <td>${data.bank}</td>
            <td>${data.nama_bank}</td>
            <td>${data.hasil}</td>
        </tr>
    `);

    window.scrollTo(0, document.body.scrollHeight);
}
