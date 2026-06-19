
async function fetchInitialState() {
    try {
        // Ambil Drop Counter awal
        const stateRes = await fetch('/api/state');
        if (stateRes.ok) {
            const stateData = await stateRes.json();
            document.getElementById('drop-counter').innerText = stateData.total_dropped;
        }

        // Ambil Policy (Kode Anda sebelumnya)
        const policyRes = await fetch('/api/policy');
        if (policyRes.ok) {
            const policyData = await policyRes.json();
            document.getElementById('policy-input').value = JSON.stringify(policyData, null, 2);
        }
    } catch (error) {
        console.error("Gagal memuat state awal:", error);
    }
}

async function updatePolicy() {
    const statusText = document.getElementById('policy-status');
    statusText.innerText = "Menyimpan...";
    statusText.style.color = "#ebcb8b"; 

    try {
        let newPolicyString = document.getElementById('policy-input').value;

        JSON.parse(newPolicyString); 

        const response = await fetch('/api/policy', {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: newPolicyString 
        });

        if (!response.ok) {
            throw new Error(`HTTP error: ${response.status}`);
        }


        statusText.innerText = "Policy berhasil diupdate!";
        statusText.style.color = "#a3be8c"; 
        
        setTimeout(() => { statusText.innerText = ""; }, 3000);

    } catch (error) {
        console.error('Update failed:', error);
        statusText.innerText = "Error: Format JSON tidak valid!";
        statusText.style.color = "#bf616a"; 
    }
}


window.onload = function () {
    fetchInitialState();
};

const updatePolicyButton = document.getElementById('btn-update-policy');
updatePolicyButton.addEventListener("click", () => updatePolicy());

const feed = document.getElementById('event-feed');
const counters = { prometheus: 0, dynatrace: 0, splunktrace: 0, riverbedtrace: 0 };

// Membuka koneksi radio ke server Go
const eventSource = new EventSource('/stream');

eventSource.onmessage = function (event) {
    const data = JSON.parse(event.data);

    if (data.type === "drop_update") {
        document.getElementById('drop-counter').innerText = data.total_dropped;
        return; 
    }

    if (data.predicted_value !== undefined) {

        if (data.source === 'prometheus') {
            if (data.metric === 'http_request_rate') {
                document.getElementById('fc-prom-req-rate').innerText = parseFloat(data.predicted_value).toFixed(2);
            }
            else if (data.metric === 'http_error_rate') {
                document.getElementById('fc-prom-error-rate').innerText = parseFloat(data.predicted_value).toFixed(2);
            }
            else if (data.metric === 'system_saturation') {
                document.getElementById('fc-prom-system-saturation').innerText = parseFloat(data.predicted_value).toFixed(2);
            }
        }
        else if (data.source === 'dynatrace') {
            document.getElementById('fc-dyna').innerText = parseFloat(data.predicted_value).toFixed(2);
        }
        else if (data.source === 'riverbedtrace') {
            document.getElementById('fc-river').innerText = parseFloat(data.predicted_value).toFixed(2);
        }


        return;
    }

    if (data.observed_value !== undefined) {
        document.getElementById('metric-source').innerText = data.source || '';
        document.getElementById('metric-name').innerText = data.metric || '';
        document.getElementById('metric-value').innerText = parseFloat(data.observed_value).toFixed(2);
        console.log("⚠️ Anomali Terdeteksi:", data.observed_value);

        return;
    }

    // 1. Update Laju Data (Counter)
    if (counters[data.source] !== undefined) {
        counters[data.source]++;
        const elId = data.source === 'splunktrace' ? 'splunk' :
            data.source === 'riverbedtrace' ? 'river' :
                data.source === 'dynatrace' ? 'dyna' : 'prom';
        document.getElementById(`rate-${elId}`).innerText = counters[data.source];
    }

    // 2. Format Waktu
    const date = new Date(data.timestamp * 1000);
    const timeStr = date.toLocaleTimeString('en-GB');

    // 3. Menentukan Isi Pesan & Prioritas
    const priority = data.payload.priority || 'P4';
    let msg = '';
    if (data.source === 'prometheus') msg = `${data.payload.metric_name}: ${parseFloat(data.payload.value).toFixed(2)}`;
    else if (data.source === 'dynatrace') msg = `CPU: ${parseFloat(data.payload.value).toFixed(2)}%`;
    else if (data.source === 'splunktrace') msg = data.payload.message;
    else if (data.source === 'riverbedtrace') msg = `RTT: ${data.payload.rtt_ms}ms`;

    // 4. Membuat Baris Tabel (Row) HTML Baru
    const tr = document.createElement('tr');
    tr.innerHTML = `
                <td>${timeStr}</td>
                <td>${data.source}</td>
                <td><span class="tag ${priority.toLowerCase()}">${priority}</span></td>
                <td>${msg}</td>
            `;



    // 5. Masukkan ke bagian atas tabel, dan hapus baris ke-51 jika ada
    feed.insertBefore(tr, feed.firstChild);
    if (feed.children.length > 50) {
        feed.removeChild(feed.lastChild);
    }
};


eventSource.onerror = function (error) {
    console.error("SSE Connection Error:", error);
};