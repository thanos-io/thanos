google.charts.load('current', {
    'packages': ['timeline']
});
google.charts.setOnLoadCallback(draw);

function draw() {
    if (thanos.refreshedAt == "0001-01-01T00:00:00Z") {
        thanos.err = "Synchronizing blocks from remote storage";
    }

    if (thanos.err != null) {
        $("#err").show().find('.alert').text(JSON.stringify(thanos.err, null, 4));
        setTimeout(function() {
            location.reload();
        }, 10000);

    } else {
        $("#err").hide();

        var container = document.getElementById('Compactions');
        var chart = new google.visualization.Timeline(container);
        var dataTable = new google.visualization.DataTable();
        var titles = {};

        dataTable.addColumn({type: 'string', id: 'Replica'});
        dataTable.addColumn({type: 'string', id: 'Label'});
        dataTable.addColumn({type: 'date', id: 'Start'});
        dataTable.addColumn({type: 'date', id: 'End'});

        dataTable.addRows(thanos.blocks
            .map(function(d) {
                // Title is the first column of the timeline.
                //
                // A unique Prometheus label that identifies each shard is used
                // as the title if present, otherwise all labels are displayed
                // externally as a legend.
                title = function() {
                    if (thanos.label != "") {
                        var key = d.thanos.labels[thanos.label];
                        if (key == undefined) {
                            throw `Label ${thanos.label} not found in ${Object.keys(d.thanos.labels)}`;
                        } else {
                            return key;
                        }
                    } else {
                        title = titles[stringify(d.thanos.labels)];
                        if (title == undefined) {
                            title = String(Object.keys(titles).length + 1);
                            titles[stringify(d.thanos.labels)] = title;
                        }
                        return title;
                    }
                }();

                label = `l: ${d.compaction.level}, res: ${d.thanos.downsample.resolution}`;
                return [title, label, new Date(d.minTime), new Date(d.maxTime)];
            }));

        chart.draw(dataTable);

        // Show external legend if no external labels were set.
        if (thanos.label == "") {
            $("#legend").show();
            for (let [key, value] of Object.entries(titles)) {
                row = `<tr> <th scope="row">${value}</th> <td>${key}</td> </tr>`;
                $("#legend table tbody").append(row);
            }
        }
    }
}

function stringify(map) {
    var t = "";
    for (let [key, value] of Object.entries(map)) {
        t += `${key}: ${value} `;
    }
    return t;
}
