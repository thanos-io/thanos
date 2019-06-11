google.charts.load('current', {
    'packages': ['timeline']
});
google.charts.setOnLoadCallback(draw);

function draw() {
    if (window.refreshedAt == "0001-01-01T00:00:00Z") {
        window.err = "Synchronizing blocks from remote storage";
    }

    if (window.err != null) {
        $("#err").show().find('.alert').text(JSON.stringify(window.err, null, 4));
        setTimeout(function() {
            location.reload();
        }, 10000);

    } else {
        $("#err").hide();

        var container = document.getElementById('Compactions');
        var chart = new google.visualization.Timeline(container);
        var dataTable = new google.visualization.DataTable();

        dataTable.addColumn({type: 'string', id: 'Replica'});
        dataTable.addColumn({type: 'string', id: 'Label'});
        dataTable.addColumn({type: 'date', id: 'Start'});
        dataTable.addColumn({type: 'date', id: 'End'});

        dataTable.addRows(blocks
            .map(function(d) {
                label = "l: " + d.compaction.level + ", res: " + d.thanos.downsample.resolution;
                return [d.thanos.labels.prometheus_replica, label, new Date(d.minTime), new Date(d.maxTime)];
            }));

        chart.draw(dataTable);
    }
}
