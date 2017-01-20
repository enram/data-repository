function getUrlParameter(name) {
    // From https://davidwalsh.name/query-string-javascript
    name = name.replace(/[\[]/, '\\[').replace(/[\]]/, '\\]');
    var regex = new RegExp('[\\?&]' + name + '=([^&#]*)');
    var results = regex.exec(location.search);
    return results === null ? '' : decodeURIComponent(results[1].replace(/\+/g, ' '));
}

var prefix = getUrlParameter('prefix').split("/");
prefix.pop(); // remove last empty element

if (prefix.length === 3) { // We are at country/radar/year level
    var expected_vp_files = 96;
    var selector = "#heatmap";
    var countryradar = prefix[0] + prefix[1]; // e.g. "deboo"
    var year = prefix[2]; // e.g. "2016"

    d3.csv("https://lw-enram.s3-eu-west-1.amazonaws.com/coverage.csv", function(error, csv) {
        if (error) throw error;

        // process coverage data
        var csv_parsed = csv.map(function(item) {
            return {
                countryradar: item.countryradar,
                date: new Date(item.date), // cast to date
                count: parseInt(item.vp_files) // cast to int
            };
        });
        var data = d3.nest()
            .key(function(d) { return d.countryradar; }) // group data by radar
            .key(function(d) { return moment(d.date).format("YYYY"); }) // group by year
            .map(csv_parsed);

        try {
            var countryRadarYearData = data[countryradar][year]; // can be out of bounds (e.g. manually entered incorrect year in querystring)

            // clear div
            d3.select(selector).html("");

            // render heatmap for czbrd in 2017
            var heatmap = calendarHeatmap()
                .data(data[countryradar][year])
                .max("expected_vp_files") // expected vp files
                .selector(selector)
                .colorRange(["#ffffff", "#428bca"])
                .tooltipEnabled("dflsmjf")
                .tooltipUnit("vp file")
                .startDate(moment(year + "-01-01").startOf('day').toDate()); // create Jan 1st date of that year
            heatmap(); // render the chart
        } catch (err) {
            if (err.name === "TypeError") {
                d3.select(selector).html("<p>😵  Oh noo, you selected something for which there are no data!</p>");
            }
        }     
    });
}
