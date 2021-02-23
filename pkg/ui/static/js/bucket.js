google.charts.load('current', {
    'packages': ['timeline']
});
google.charts.setOnLoadCallback(draw);

function draw() {
    if (thanos.refreshedAt == "0001-01-01T00:00:00Z") {
        thanos.err = "Synchronizing blocks from remote storage";
    }
    else if (!thanos.err && thanos.blocks.length == 0) {
        thanos.err = "No blocks are currently loaded";
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
        dataTable.addColumn({type: 'string', role: 'tooltip'});
        dataTable.addColumn({type: 'date', id: 'Start'});
        dataTable.addColumn({type: 'date', id: 'End'});

        dataTable.addRows(thanos.blocks
            .sort((a, b) => a.thanos.downsample.resolution - b.thanos.downsample.resolution)
            .map(function(d) {
                if (!isThanosBlock(d)) {
                    title = 'Prometheus'
                    titles['Prometheus blocks with no Thanos data'] = title

                    return [title, `level: ${d.compaction.level}`, generateTooltip(d), new Date(d.minTime), new Date(d.maxTime)]
                }
                // Title is the first column of the timeline.
                //
                // A unique Prometheus label that identifies each shard is used
                // as the title if present, otherwise labels are displayed
                // externally as a legend.
                title = function() {
                    var key = thanos.label != "" && d.thanos.labels[thanos.label];

                    if (key) {
                        return key;
                    } else {
                        title = titles[stringify(d.thanos.labels)];
                        if (title == undefined) {
                            title = String(Object.keys(titles).length + 1);
                            titles[stringify(d.thanos.labels)] = title;
                        }
                        return title;
                    }
                }();

                label = `level: ${d.compaction.level}, resolution: ${d.thanos.downsample.resolution}`;
                return [title, label, generateTooltip(d), new Date(d.minTime), new Date(d.maxTime)];
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

// isThanosBlock returns true if it is a block that has been processed by Thanos.
function isThanosBlock(d) {
    return d.thanos.labels !== null &&
        d.thanos.source !== ""
}

function stringify(map) {
    var t = "";
    for (let [key, value] of Object.entries(map)) {
        t += `${key}: ${value} `;
    }
    return t;
}

function generateTooltip(block) {
    var tooltip = document.createElement("div");
    tooltip.className = "card";


    var title = document.createElement("h6");
    title.className = "card-header text-nowrap";
    title.innerHTML = block.ulid;

    var info = document.createElement("ul");
    info.className = "list-group list-group-flush";


    if (isThanosBlock(block)) {
        var metaInfo = document.createElement("li");
        metaInfo.className = "list-group-item";
        metaInfo.innerHTML = "<p><b>Labels</b></p>";

        var labelTable = document.createElement("table");
        labelTable.className = "table table-sm mb-0";
        for (let [key, value] of Object.entries(block.thanos.labels)) {
            labelTable.innerHTML += `<tr><td>${key}</td><td>${value}</td></tr>`;
        }
        metaInfo.appendChild(labelTable);
    }

    var dateInfo = document.createElement("li");
    var minTime = new Date(block.minTime);
    var maxTime = new Date(block.maxTime);

    dateInfo.className = "list-group-item";
    dateInfo.innerHTML = generateLine("Start Date: ", minTime.toLocaleDateString() + " " + minTime.toLocaleTimeString());
    dateInfo.innerHTML += generateLine("End Date: ", maxTime.toLocaleDateString() + " " + maxTime.toLocaleTimeString());
    dateInfo.innerHTML += generateLine("Duration: ", moment.duration(maxTime-minTime).humanize());

    var statsInfo = document.createElement("li");
    statsInfo.className = "list-group-item";
    statsInfo.innerHTML = generateLine("Series: ", block.stats.numSeries.toLocaleString());
    statsInfo.innerHTML += generateLine("Samples: ", block.stats.numSamples.toLocaleString());
    statsInfo.innerHTML += generateLine("Chunks: ", block.stats.numChunks.toLocaleString());


    if (isThanosBlock(block)) {
        info.appendChild(metaInfo);
    }

    info.appendChild(dateInfo);
    info.appendChild(statsInfo);

    if (isThanosBlock(block)) {
        var compactInfo = document.createElement("li");
        compactInfo.className = "list-group-item";
        compactInfo.innerHTML = generateLine("Resolution: ", block.thanos.downsample.resolution);
        compactInfo.innerHTML += generateLine("Level: ", block.compaction.level);
        compactInfo.innerHTML += generateLine("Source: ", block.thanos.source);

        info.appendChild(compactInfo);
    }

    tooltip.appendChild(title);
    tooltip.appendChild(info);

    return tooltip.outerHTML;
}

function generateLine(key, value) {
    return "<b>" + key + "</b>" + value + "</br>"
}
