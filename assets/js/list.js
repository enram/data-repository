if (typeof S3BL_IGNORE_PATH === 'undefined' || S3BL_IGNORE_PATH !== true) {
  var S3BL_IGNORE_PATH = false;
}

if (typeof BUCKET_URL === 'undefined') {
  var BUCKET_URL = location.protocol + '//' + location.hostname;
}

if (typeof BUCKET_NAME !== 'undefined') {
  // if bucket_url does not start with bucket_name, assume path-style url
  if (!~BUCKET_URL.indexOf(location.protocol + '//' + BUCKET_NAME)) {
    BUCKET_URL += '/' + BUCKET_NAME;
  }
}

if (typeof BUCKET_WEBSITE_URL === 'undefined') {
  var BUCKET_WEBSITE_URL = BUCKET_URL;
}

if (typeof S3B_ROOT_DIR === 'undefined') {
  var S3B_ROOT_DIR = '';
}

if (typeof S3B_SORT === 'undefined') {
  var S3B_SORT = 'DEFAULT';
}

jQuery(function($) {
  getS3Data();
});

// This will sort your file listing by most recently modified.
// Flip the comparator to '>' if you want oldest files first.
function sortFunction(a, b) {
  switch (S3B_SORT) {
    case "OLD2NEW":
      return a.LastModified > b.LastModified ? 1 : -1;
    case "NEW2OLD":
      return a.LastModified < b.LastModified ? 1 : -1;
    case "A2Z":
      return a.Key < b.Key ? 1 : -1;
    case "Z2A":
      return a.Key > b.Key ? 1 : -1;
    case "BIG2SMALL":
      return a.Size < b.Size ? 1 : -1;
    case "SMALL2BIG":
      return a.Size > b.Size ? 1 : -1;
  }
}
function getS3Data(marker, table_rows) {
  // If file listing is untruncated (= fewer items than delimiter) than 
  // this function will only run once. If truncated, this function will run 
  // recursively, passing forward marker and already created table_rows.
  // By default, recursive running will only be triggered on > 1000 files
  // in one directory.

  var s3_rest_url = createS3QueryUrl(marker);

 // set loading notice
  $('#listing').html('<p>loading...</p>');
  
  $.get(s3_rest_url)
    .done(function(data) {
      // clear loading notice
      $('#listing').html('');
      var xml = $(data);
      var info = getInfoFromS3Data(xml);

      if (S3B_SORT != 'DEFAULT') {
        var sortedFiles = info.files;
        sortedFiles.sort(sortFunction);
        info.files = sortedFiles;
      }
      
      // build table_rows
      if (typeof table_rows === 'undefined') { // 1st trunc
        table_rows = buildRows(info, true);
      } else {
        table_rows = table_rows + buildRows(info, false);
      }

      // get more truncs
      if (info.nextMarker !== 'null') {
        getS3Data(info.nextMarker, table_rows);
      } else { // no more truncs

        // render bucket link
        $('#bucket_link').html('<a href="' + BUCKET_WEBSITE_URL + '">' + BUCKET_WEBSITE_URL + '</a>');

        // render breadcrumbs
        var nav_html = '<ol class="breadcrumb">You\'re here: \n';
        nav_html += buildNavigation(info);
        nav_html += '</ol>\n';

        $('#breadcrumbs').html(nav_html);

        // render table
        var table_html = '<table class="table table-striped table-condensed"><tbody>\n';
        table_html += '<tr><th>Name</th><th>Last modified</th><th>Size</th></tr>\n';
        table_html += table_rows;
        table_html += '</tbody></table>\n';

        $('#listing').html(table_html);
      }
    })  
    .fail(function(error) {
      console.error(error);
      $('#listing').html('<p class="alert alert-danger">Error: ' + error + '</p>');
    });
}

function createS3QueryUrl(marker) {
  var s3_rest_url = BUCKET_URL;
  s3_rest_url += '?delimiter=/';

  //
  // Handling paths and prefixes:
  //
  // 1. S3BL_IGNORE_PATH = false
  // Uses the pathname
  // {bucket}/{path} => prefix = {path}
  //
  // 2. S3BL_IGNORE_PATH = true
  // Uses ?prefix={prefix}
  //
  // Why both? Because we want classic directory style listing in normal
  // buckets but also allow deploying to non-buckets
  //

  var rx = '.*[?&]prefix=' + S3B_ROOT_DIR + '([^&]+)(&.*)?$';
  var prefix = '';
  if (S3BL_IGNORE_PATH === false) {
    prefix = location.pathname.replace(/^\//, S3B_ROOT_DIR);
  }
  var match = location.search.match(rx);
  if (match) {
    prefix = S3B_ROOT_DIR + match[1];
  } else {
    if (S3BL_IGNORE_PATH) {
      prefix = S3B_ROOT_DIR;
    }
  }
  if (prefix) {
    // make sure we end in /
    prefix = prefix.replace(/\/$/, '') + '/';
    s3_rest_url += '&prefix=' + prefix;
  }
  if (marker) {
    s3_rest_url += '&marker=' + marker;
  }
  return s3_rest_url;
}

function getInfoFromS3Data(xml) {
  var files = $.map(xml.find('Contents'), function(item) {
    item = $(item);
    return {
      Key: item.find('Key').text(),
          LastModified: item.find('LastModified').text(),
          Size: bytesToHumanReadable(item.find('Size').text()),
          Type: 'file'
    };
  });
  var directories = $.map(xml.find('CommonPrefixes'), function(item) {
    item = $(item);
    return {
      Key: item.find('Prefix').text(),
        LastModified: '',
        Size: '',
        Type: 'directory'
    };
  });
  var nextMarker = null;
  if ($(xml.find('IsTruncated')[0]).text() === 'true') {
    nextMarker = $(xml.find('NextMarker')[0]).text();
  } else {
    nextMarker = null;
  }
  return {
    files: files,
    directories: directories,
    prefix: $(xml.find('Prefix')[0]).text(),
    nextMarker: encodeURIComponent(nextMarker)
  };
}

function buildNavigation(info) {
  var html_list = [];
  var directories = info.prefix.split('/');
  var directoryName = '';
  var directoryPath = '';

  directories.unshift(''); // add root dir as first directory
  directories.pop(); // remove empty element after last slash

  jQuery.each(directories, function(index, directory) {
    if (index === 0) { // root directory
      directoryName = '<span class="glyphicon glyphicon-home" aria-hidden="true"></span>';
      directoryPath = directoryPath;
    } else {
      directoryName = directory;
      directoryPath = directoryPath + encodeURIComponent(directory) + '/';
    }

    if (index === directories.length - 1) { // last directory
      html_list.push('<li class="active">' + directoryName + '</li>\n'); // don't display as link
    } else {
      html_list.push('<li><a href="?prefix=' + directoryPath + '">' + directoryName + '</a></li>\n');
    }
  });

  return html_list.join('');
}

function buildRows(info, first_iteration) {
  var files = info.files.concat(info.directories);
  var prefix = info.prefix;
  var html_list = [];

  function buildRow(item) {
    var row = '';
    row += '<tr>';
    row += '<td><a href="' + item.href + '">' + item.keyText + '</a></td>';
    row += '<td>' + item.LastModified + '</td>';
    row += '<td>' + item.Size + '</td>';
    row += '</tr>\n';
    return row;
  }

  // add parent directory item (../) at the start of the dir listing (first_iteration = true), 
  // unless we are already at root dir
  if (first_iteration && prefix && prefix !== S3B_ROOT_DIR) {
    var up = prefix.replace(/\/$/, '').split('/').slice(0, -1).concat('').join('/'),  // one directory up
      item =
          {
            Key: up,
            LastModified: '',
            Size: '',
            keyText: '⏎',
            href: S3BL_IGNORE_PATH ? '?prefix=' + up : '../'
          },
      row = buildRow(item);
    html_list.push(row);
  }

  // list items
  jQuery.each(files, function(idx, item) {
    item.keyText = item.Key.substring(prefix.length);
    if (item.Type === 'directory') { // directory
      if (S3BL_IGNORE_PATH) {
        item.href = '?prefix=' + item.Key;
      } else {
        item.href = item.keyText;
      }
    } else { // file
      item.href = BUCKET_WEBSITE_URL + '/' + encodeURIComponent(item.Key);
      item.href = item.href.replace(/%2F/g, '/');
    }
    var row = buildRow(item);
    html_list.push(row);
  });

  return html_list.join('');
}

function bytesToHumanReadable(sizeInBytes) {
  var i = -1;
  var units = [' kB', ' MB', ' GB'];
  do {
    sizeInBytes = sizeInBytes / 1024;
    i++;
  } while (sizeInBytes > 1024);
  return Math.max(sizeInBytes, 0.1).toFixed(1) + units[i];
}
