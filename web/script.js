
async function fetchInitialState() {
    try {
        const stateRes = await fetch('/api/state');
        if (stateRes.ok) {
            const stateData = await stateRes.json();
            const dropEl = document.getElementById('drop-counter');
            if (dropEl) dropEl.innerText = stateData.total_dropped;

            if (stateData.ingestion_totals) {
                counters.prometheus = stateData.ingestion_totals.prometheus || 0;
                counters.dynatrace = stateData.ingestion_totals.dynatrace || 0;
                counters.splunktrace = stateData.ingestion_totals.splunktrace || 0;
                counters.riverbedtrace = stateData.ingestion_totals.riverbedtrace || 0;

                document.getElementById('rate-prom').innerText = counters.prometheus;
                document.getElementById('rate-dyna').innerText = counters.dynatrace;
                document.getElementById('rate-splunk').innerText = counters.splunktrace;
                document.getElementById('rate-river').innerText = counters.riverbedtrace;
            }
        }

        const policyRes = await fetch('/api/policy');
        if (policyRes.ok) {
            const policyData = await policyRes.json();
            document.getElementById('policy-input').value = JSON.stringify(policyData, null, 2);
        }
    } catch (error) {
        console.error("Failed to Load Inital State:", error);
    }
}

async function updatePolicy() {
    const statusText = document.getElementById('policy-status');
    statusText.innerText = "Saving...";
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


        statusText.innerText = "Policy Updated!";
        statusText.style.color = "#a3be8c";

        setTimeout(() => { statusText.innerText = ""; }, 3000);

    } catch (error) {
        console.error('Update failed:', error);
        statusText.innerText = "Error: INVALID JSON FORMAT!";
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

            if (data.metric == 'rtt_ms') {
                document.getElementById('fc-river-rtt').innerText = parseFloat(data.predicted_value).toFixed(2);
            }
            if (data.metric == 'throughput_mbps') {
                document.getElementById('fc-river-throughput').innerText = parseFloat(data.predicted_value).toFixed(2);
            }
            if (data.metric == 'packet_loss_percent') {
                document.getElementById('fc-river-packet-loss').innerText = parseFloat(data.predicted_value).toFixed(2);
            }
        }


        return;
    }

    if (data.observed_value !== undefined) {
        document.getElementById('metric-source').innerText = data.source || '';
        document.getElementById('metric-name').innerText = data.metric || '';
        document.getElementById('metric-value').innerText = parseFloat(data.observed_value).toFixed(2);
        console.log("Anomaly Detected:", data.observed_value);

        return;
    }

    if (counters[data.source] !== undefined) {
        counters[data.source]++;
        const elId = data.source === 'splunktrace' ? 'splunk' :
            data.source === 'riverbedtrace' ? 'river' :
                data.source === 'dynatrace' ? 'dyna' : 'prom';
        document.getElementById(`rate-${elId}`).innerText = counters[data.source];
    }

    const date = new Date(data.timestamp * 1000);
    const timeStr = date.toLocaleTimeString('en-GB');

    const priority = data.payload.priority || 'P4';
    let msg = '';
    if (data.source === 'prometheus') msg = `${data.payload.metric_name}: ${parseFloat(data.payload.value).toFixed(2)}`;
    else if (data.source === 'dynatrace') msg = `CPU: ${parseFloat(data.payload.value).toFixed(2)}%`;
    else if (data.source === 'splunktrace') msg = data.payload.message;
    else if (data.source === 'riverbedtrace') msg = `RTT: ${data.payload.rtt_ms}ms`;

    const tr = document.createElement('tr');
    tr.innerHTML = `
                <td>${timeStr}</td>
                <td>${data.source}</td>
                <td><span class="tag ${priority.toLowerCase()}">${priority}</span></td>
                <td>${msg}</td>
            `;


    feed.insertBefore(tr, feed.firstChild);
    if (feed.children.length > 50) {
        feed.removeChild(feed.lastChild);
    }
};


eventSource.onerror = function (error) {
    console.error("SSE Connection Error:", error);
};