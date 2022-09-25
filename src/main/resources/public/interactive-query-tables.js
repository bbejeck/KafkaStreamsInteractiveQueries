const symbols = ["AACO", "GOOG", "CFLT", "MSFT", "WKRP"];


function loadIqTables() {
    $.getJSON("http://localhost:7076/streams-iq/v2/range", function (response) {
        updateRangeTable(response, $('#rangeTable'))
        $('#rangeHeader').animate({color: 'red'}, 500).animate({color: '#CCCCCC'}, 500)
    });

    updateKeyTableWithList("http://localhost:7076/streams-iq/v1/get/", symbols, $('#keyTable'), $('#keyHeader'));
}

function updateKeyTableWithList(path, values, table, header) {
    $.each(values, function (index, value) {
        $.getJSON(path + value, function (response) {
            updateKeyTable(response, table)
        })
    });
}

function updateRangeTable(response, table) {
    $.each(response.result, function (index, element) {
        let key = element.symbol;
        if ($("tr[id='" + key + "_range']").length) {
            $("td[id='" + key + "_td_range']").text(parseFloat(element.buys + element.sells).toFixed(2))
        } else {
            let row = $('<tr id="' + key + '_range">');
            row.append($('<td>' + key + '</td><td id="' + key + '_td_range">' + parseFloat(element.buys + element.sells).toFixed(2) + '</td>'));
            table.append(row);
        }
    });
}


function updateKeyTable(response, table) {
    if (!response.errorMessage) {
        let key = response.result.symbol;
        let row = $("tr[id='" + key + "_key']");
        if (row.length) {
            $("td[id='" + key + "_td_key']").text(parseFloat(response.result.sells).toFixed(2))
        } else {
            row = $('<tr id="' + key + '_key">');
            row.append($('<td>' + key + '</td><td id="' + key + '_td_key">' + parseFloat(response.result.sells).toFixed(2) + '</td>'));
            table.append(row);
        }
    }
}