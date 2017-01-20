d3.csv("https://lw-enram.s3-eu-west-1.amazonaws.com/coverage.csv", function(error, csv) {
    if (error) throw error;

    var expected_vp_files = 96;

    // process coverage data
    var csv_parsed = csv.map(function(item) {
        var vp_files = parseInt(item.vp_files); // cast to int

        return {
            radar: item.countryradar,
            date: new Date(item.date), // cast to date
            vp_files: vp_files,
            count: vp_files / expected_vp_files // percentage
        };
    });
    var data = d3.nest()
        .key(function(d) { return d.radar; }) // group data by radar
        .key(function(d) { return moment(d.date).format("YYYY"); }) // group by year
        .map(csv_parsed);

    // render heatmap for czbrd in 2017
    var heatmap = calendarHeatmap()
        .data(data["czbrd"]["2017"])
        .selector("#heatmap")
        .colorRange(["#f4f7f7", "#79a8a9"])
        .tooltipEnabled(true)
        .tooltipUnit("coverage");

    heatmap(); // render the chart
});
