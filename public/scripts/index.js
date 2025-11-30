document.addEventListener('DOMContentLoaded', function() {
    const tabs = document.querySelectorAll('.tab_navigation li');
    const sections = document.querySelectorAll('main');
    
    sections.forEach((section, i) => {
        section.style.display = i === 0 ? 'block' : 'none';
    });
    
    tabs.forEach((tab, index) => {
        tab.addEventListener('click', function() {
            tabs.forEach(t => t.removeAttribute('id'));
            this.id = 'selected';
            
            sections.forEach(s => s.style.display = 'none');
            const targetId = 'tab_' + (index + 1);
            const targetSection = document.getElementById(targetId);
            if (targetSection) {
                targetSection.style.display = 'block';
                
                // Load data based on which tab is active
                if (index === 0) {
                    loadAllAnalytics();
                } else if (index === 1) {
                    loadTab2Data();
                } else if (index === 2) {
                    loadTab3Data();
                }
            }
        });
    });
    
    // Load initial data for first tab
    loadAllAnalytics();
});

async function loadAllAnalytics() {
    try {
        const [
            artists, 
            songs, 
            albums,
            sameTopArtist, 
            mentionsArtists, 
            mentionsMetrics, 
            longTail
        ] = await Promise.all([
            fetch('/api/top-artists').then(res => res.json()),
            fetch('/api/top-songs').then(res => res.json()),
            fetch('/api/top-albums').then(res => res.json()),
            fetch('/api/same-top-artist').then(res => res.json()),
            fetch('/api/mentions-artist').then(res => res.json()),
            fetch('/api/mentions-metrics').then(res => res.json()),
            fetch('/api/long-tail').then(res => res.json())
        ]);

        renderTopChart('top-artists-chart', artists, 'Top 20 General Artists');
        renderTopChart('top-songs-chart', songs, 'Top 20 General Songs');
        renderTopChart('top-albums-chart', albums, 'Top 20 General Albums');
        renderQuery4('query4-container', sameTopArtist);
        renderQuery5('query5-container', mentionsArtists, mentionsMetrics);
        renderQuery6('query6-container', longTail);
    } catch (error) {
        console.error('Error loading analytics data:', error);
    }
}

function renderTopChart(containerId, data, title) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = '';

    if (!data || data.length === 0) {
        container.innerHTML = '<p>No data available</p>';
        return;
    }

    const chartContainer = document.createElement('div');
    chartContainer.className = 'top-chart-container';

    const titleElement = document.createElement('h2');
    titleElement.textContent = title;
    titleElement.className = 'chart-title';
    chartContainer.appendChild(titleElement);

    const chartContent = document.createElement('div');
    chartContent.className = 'chart-content';

    const barSection = document.createElement('div');
    barSection.className = 'bar-section';

    const pieSection = document.createElement('div');
    pieSection.className = 'pie-section';

    const maxListeners = Math.max(...data.map(item => item.listeners));
    data.forEach((item, index) => {
        const barItem = document.createElement('div');
        barItem.className = 'bar-item';

        const barLabel = document.createElement('span');
        barLabel.className = 'bar-label';
        barLabel.textContent = item.name.length > 25 ? item.name.substring(0, 25) + '...' : item.name;
        barLabel.title = item.name;

        const barContainer = document.createElement('div');
        barContainer.className = 'bar-container';

        const bar = document.createElement('div');
        bar.className = 'bar';
        bar.style.width = `${(item.listeners / maxListeners) * 100}%`;

        const barValue = document.createElement('span');
        barValue.className = 'bar-value';
        barValue.textContent = item.listeners.toLocaleString();

        barContainer.appendChild(bar);
        barContainer.appendChild(barValue);
        barItem.appendChild(barLabel);
        barItem.appendChild(barContainer);
        barSection.appendChild(barItem);
    });

    const pieChartContainer = document.createElement('div');
    pieChartContainer.className = 'pie-chart-container';
    
    const pieChart = document.createElement('div');
    pieChart.className = 'pie-chart';
    pieChart.id = `${containerId}-pie`;
    
    let cumulativePercentage = 0;
    const totalParticipation = data.reduce((sum, item) => sum + item.participation_percentage, 0);
    
    let conicGradient = '';
    data.forEach((item, index) => {
        const percentage = (item.participation_percentage / totalParticipation) * 100;
        const start = cumulativePercentage;
        const end = cumulativePercentage + percentage;
        
        conicGradient += `${getColor(index)} ${start}% ${end}%`;
        if (index < data.length - 1) {
            conicGradient += ', ';
        }
        
        pieChart.style.setProperty(`--color${index}`, getColor(index));
        pieChart.style.setProperty(`--percentage${index}`, `${end}%`);
        
        cumulativePercentage = end;
    });
    
    pieChart.style.background = `conic-gradient(${conicGradient})`;
    
    pieChartContainer.appendChild(pieChart);
    
    setTimeout(() => {
        setupPieChartHover(`${containerId}-pie`, data);
    }, 100);

    pieSection.appendChild(pieChartContainer);
    
    chartContent.appendChild(barSection);
    chartContent.appendChild(pieSection);
    chartContainer.appendChild(chartContent);
    container.appendChild(chartContainer);
}

function renderQuery4(containerId, data) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = '';

    const titleElement = document.createElement('h2');
    titleElement.textContent = 'Users with the same top 1 artist';
    titleElement.className = 'chart-title';
    container.appendChild(titleElement);

    if (!data || !data.name) {
        container.innerHTML += '<p>No data available</p>';
        return;
    }

    const table = document.createElement('table');
    table.className = 'stats-table';

    const headerRow = document.createElement('tr');
    headerRow.innerHTML = `
        <th>Mode</th>
        <th>Frequency</th>
    `;
    table.appendChild(headerRow);

    const dataRow = document.createElement('tr');
    dataRow.innerHTML = `
        <td>${data.name}</td>
        <td>${data.frequency.toLocaleString()}</td>
    `;
    table.appendChild(dataRow);

    container.appendChild(table);
}

function renderQuery5(containerId, mentionsData, metricsData) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = '';

    const titleElement = document.createElement('h2');
    titleElement.textContent = 'Distribution of mentions per artist';
    titleElement.className = 'chart-title';
    container.appendChild(titleElement);

    if (!mentionsData || mentionsData.length === 0) {
        container.innerHTML += '<p>No data available</p>';
        return;
    }

    const contentDiv = document.createElement('div');
    contentDiv.className = 'query5-content';

    const metricsDiv = document.createElement('div');
    metricsDiv.className = 'metrics-section';

    if (metricsData && metricsData.average) {
        metricsDiv.innerHTML = `
            <h3>Statistical Metrics</h3>
            <div class="metrics-grid">
                <div class="metric-item">
                    <span class="metric-label">Average:</span>
                    <span class="metric-value">${metricsData.average.toFixed(2)}</span>
                </div>
                <div class="metric-item">
                    <span class="metric-label">Median:</span>
                    <span class="metric-value">${metricsData.median.toFixed(2)}</span>
                </div>
                <div class="metric-item">
                    <span class="metric-label">Standard Deviation:</span>
                    <span class="metric-value">${metricsData.standard_deviation.toFixed(2)}</span>
                </div>
            </div>
        `;
    }

    const histogramDiv = document.createElement('div');
    histogramDiv.className = 'histogram-section';

    const histogramTitle = document.createElement('h3');
    histogramTitle.textContent = 'Top 100 Artists by Mentions';
    histogramDiv.appendChild(histogramTitle);

    const maxMentions = Math.max(...mentionsData.map(item => item.mentions));
    
    mentionsData.forEach((item, index) => {
        const barItem = document.createElement('div');
        barItem.className = 'histogram-bar-item';

        const barLabel = document.createElement('span');
        barLabel.className = 'histogram-bar-label';
        barLabel.textContent = `${index + 1}. ${item.name.length > 20 ? item.name.substring(0, 20) + '...' : item.name}`;
        barLabel.title = item.name;

        const barContainer = document.createElement('div');
        barContainer.className = 'histogram-bar-container';

        const bar = document.createElement('div');
        bar.className = 'histogram-bar';
        bar.style.width = `${(item.mentions / maxMentions) * 100}%`;

        const barValue = document.createElement('span');
        barValue.className = 'histogram-bar-value';
        barValue.textContent = item.mentions.toLocaleString();

        barContainer.appendChild(bar);
        barContainer.appendChild(barValue);
        barItem.appendChild(barLabel);
        barItem.appendChild(barContainer);
        histogramDiv.appendChild(barItem);
    });

    contentDiv.appendChild(metricsDiv);
    contentDiv.appendChild(histogramDiv);
    container.appendChild(contentDiv);
}

function renderQuery6(containerId, data) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = '';

    const titleElement = document.createElement('h2');
    titleElement.textContent = 'Long tail';
    titleElement.className = 'chart-title';
    container.appendChild(titleElement);

    if (!data || data.value === undefined) {
        container.innerHTML += '<p>No data available</p>';
        return;
    }

    const percentageDiv = document.createElement('div');
    percentageDiv.className = 'long-tail-percentage';

    const percentageValue = document.createElement('div');
    percentageValue.className = 'percentage-value';
    percentageValue.textContent = `${data.value.toFixed(2)}%`;

    const percentageLabel = document.createElement('div');
    percentageLabel.className = 'percentage-label';
    percentageLabel.textContent = 'of artists hold 80% of mentions';

    percentageDiv.appendChild(percentageValue);
    percentageDiv.appendChild(percentageLabel);
    container.appendChild(percentageDiv);
}

function setupPieChartHover(pieChartId, data) {
    const pieChart = document.getElementById(pieChartId);
    if (!pieChart) return;

    const tooltip = $('#pie-tooltip');
    const totalParticipation = data.reduce((sum, item) => sum + item.participation_percentage, 0);

    $(pieChart).on('mousemove', function(e) {
        const rect = this.getBoundingClientRect();
        const centerX = rect.left + rect.width / 2;
        const centerY = rect.top + rect.height / 2;
        const mouseX = e.clientX - centerX;
        const mouseY = e.clientY - centerY;
        
        let angle = Math.atan2(mouseY, mouseX) * 180 / Math.PI;
        if (angle < 0) angle += 360;
        
        let cumulativePercentage = 0;
        let foundItem = null;
        
        for (let i = 0; i < data.length; i++) {
            const percentage = (data[i].participation_percentage / totalParticipation) * 100;
            const segmentStart = cumulativePercentage;
            const segmentEnd = cumulativePercentage + percentage;
            
            if (angle >= (segmentStart * 3.6) && angle < (segmentEnd * 3.6)) {
                foundItem = data[i];
                break;
            }
            cumulativePercentage = segmentEnd;
        }
        
        if (foundItem) {
            const percentage = (foundItem.participation_percentage / totalParticipation) * 100;
            tooltip.html(`
                <strong>${foundItem.name}</strong><br>
                Listeners: ${foundItem.listeners.toLocaleString()}<br>
                Participation: ${percentage.toFixed(2)}%
            `).show().css({
                left: e.pageX + 10,
                top: e.pageY + 10
            });
        } else {
            tooltip.hide();
        }
    });

    $(pieChart).on('mouseleave', function() {
        tooltip.hide();
    });
}

// Load data for Tab 2 (Queries 7-10)
async function loadTab2Data() {
    try {
        const [
            itemsMetrics,
            uniqueItems,
            duplicatedArtists,
            duplicatedAlbums,
            duplicatedSongs,
            loyalListeners
        ] = await Promise.all([
            fetch('/api/items-per-user-metrics').then(res => res.json()),
            fetch('/api/unique-items').then(res => res.json()),
            fetch('/api/duplicated-artists').then(res => res.json()),
            fetch('/api/duplicated-albums').then(res => res.json()),
            fetch('/api/duplicated-songs').then(res => res.json()),
            fetch('/api/loyal-listeners').then(res => res.json())
        ]);

        renderQuery7('query7-container', itemsMetrics);
        renderQuery8('query8-container', uniqueItems);
        renderQuery9('query9-artists-container', duplicatedArtists, 'Top 10 Duplicated Artists');
        renderQuery9('query9-albums-container', duplicatedAlbums, 'Top 10 Duplicated Albums');
        renderQuery9('query9-songs-container', duplicatedSongs, 'Top 10 Duplicated Songs');
        renderQuery10('query10-container', loyalListeners);
    } catch (error) {
        console.error('Error loading tab 2 data:', error);
    }
}

// Load data for Tab 3 (Queries 11-12)
async function loadTab3Data() {
    try {
        const [
            pairedArtists,
            trioArtists
        ] = await Promise.all([
            fetch('/api/paired-artists').then(res => res.json()),
            fetch('/api/trio-artists').then(res => res.json())
        ]);

        renderQuery11('query11-container', pairedArtists);
        renderQuery12('query12-container', trioArtists);
    } catch (error) {
        console.error('Error loading tab 3 data:', error);
    }
}

// Query 7: Items per user
function renderQuery7(containerId, data) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = '';

    const titleElement = document.createElement('h2');
    titleElement.textContent = 'Items per user';
    titleElement.className = 'chart-title';
    container.appendChild(titleElement);

    if (!data || data.average === undefined) {
        container.innerHTML += '<p>No data available</p>';
        return;
    }

    const metricsDiv = document.createElement('div');
    metricsDiv.className = 'simple-metrics';

    metricsDiv.innerHTML = `
        <div class="metric-item-large">
            <span class="metric-label-large">Average Items per User:</span>
            <span class="metric-value-large">${data.average.toFixed(2)}</span>
        </div>
        <div class="metric-item-large">
            <span class="metric-label-large">Median Items per User:</span>
            <span class="metric-value-large">${data.median.toFixed(2)}</span>
        </div>
    `;

    container.appendChild(metricsDiv);
}

// Query 8: Unique Items
function renderQuery8(containerId, data) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = '';

    const titleElement = document.createElement('h2');
    titleElement.textContent = 'Unique Items per User';
    titleElement.className = 'chart-title';
    container.appendChild(titleElement);

    if (!data || data.length === 0) {
        container.innerHTML += '<p>No data available</p>';
        return;
    }

    const tableContainer = document.createElement('div');
    tableContainer.className = 'table-container';

    const table = document.createElement('table');
    table.className = 'unique-items-table';

    const headerRow = document.createElement('tr');
    headerRow.innerHTML = `
        <th>User ID</th>
        <th>Artists</th>
        <th>Songs</th>
        <th>Albums</th>
    `;
    table.appendChild(headerRow);

    data.forEach((item, index) => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${item.user_id}</td>
            <td>${item.artists.toLocaleString()}</td>
            <td>${item.songs.toLocaleString()}</td>
            <td>${item.albums.toLocaleString()}</td>
        `;
        table.appendChild(row);
    });

    tableContainer.appendChild(table);
    container.appendChild(tableContainer);
}

// Query 9: Duplicated items (artists, albums, songs)
function renderQuery9(containerId, data, title) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = '';

    const titleElement = document.createElement('h2');
    titleElement.textContent = title;
    titleElement.className = 'chart-title';
    container.appendChild(titleElement);

    if (!data || data.length === 0) {
        container.innerHTML += '<p>No data available</p>';
        return;
    }

    const tableContainer = document.createElement('div');
    tableContainer.className = 'table-container';

    const table = document.createElement('table');
    table.className = 'query9-table'; // Changed from triplets-table to query9-table

    const headerRow = document.createElement('tr');
    if (title.includes('Artists')) {
        headerRow.innerHTML = `
            <th>Artist 1</th>
            <th>Artist 2</th>
            <th>Artist 3</th>
            <th>Total Users</th>
        `;
    } else if (title.includes('Albums')) {
        headerRow.innerHTML = `
            <th>Album 1</th>
            <th>Album 2</th>
            <th>Album 3</th>
            <th>Total Users</th>
        `;
    } else {
        headerRow.innerHTML = `
            <th>Song 1</th>
            <th>Song 2</th>
            <th>Song 3</th>
            <th>Total Users</th>
        `;
    }
    table.appendChild(headerRow);

    data.forEach((item, index) => {
        const row = document.createElement('tr');
        const item1 = title.includes('Artists') ? item.artist1 : title.includes('Albums') ? item.album1 : item.song1;
        const item2 = title.includes('Artists') ? item.artist2 : title.includes('Albums') ? item.album2 : item.song2;
        const item3 = title.includes('Artists') ? item.artist3 : title.includes('Albums') ? item.album3 : item.song3;
        
        row.innerHTML = `
            <td>${truncateText(item1, 30)}</td>
            <td>${truncateText(item2, 30)}</td>
            <td>${truncateText(item3, 30)}</td>
            <td>${item.total_users.toLocaleString()}</td>
        `;
        table.appendChild(row);
    });

    tableContainer.appendChild(table);
    container.appendChild(tableContainer);
}

// Query 10: Loyal listeners
function renderQuery10(containerId, data) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = '';

    const titleElement = document.createElement('h2');
    titleElement.textContent = 'Loyal listeners';
    titleElement.className = 'chart-title';
    container.appendChild(titleElement);

    if (!data || data.value === undefined) {
        container.innerHTML += '<p>No data available</p>';
        return;
    }

    const loyalDiv = document.createElement('div');
    loyalDiv.className = 'loyal-listeners';

    loyalDiv.innerHTML = `
        <div class="loyal-value">${data.value.toLocaleString()}</div>
        <div class="loyal-label">users are loyal to their top artist</div>
    `;

    container.appendChild(loyalDiv);
}

// Query 11: Paired artists
function renderQuery11(containerId, data) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = '';

    const titleElement = document.createElement('h2');
    titleElement.textContent = 'Top 50 Paired Artists';
    titleElement.className = 'chart-title';
    container.appendChild(titleElement);

    if (!data || data.length === 0) {
        container.innerHTML += '<p>No data available</p>';
        return;
    }

    const tableContainer = document.createElement('div');
    tableContainer.className = 'table-container';

    const table = document.createElement('table');
    table.className = 'pairs-table';

    const headerRow = document.createElement('tr');
    headerRow.innerHTML = `
        <th>Artist 1</th>
        <th>Artist 2</th>
        <th>Total Users</th>
    `;
    table.appendChild(headerRow);

    data.forEach((item, index) => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${truncateText(item.artist1, 40)}</td>
            <td>${truncateText(item.artist2, 40)}</td>
            <td>${item.total_users.toLocaleString()}</td>
        `;
        table.appendChild(row);
    });

    tableContainer.appendChild(table);
    container.appendChild(tableContainer);
}

// Query 12: Trio artists
function renderQuery12(containerId, data) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = '';

    const titleElement = document.createElement('h2');
    titleElement.textContent = 'Top 20 Trio Artists';
    titleElement.className = 'chart-title';
    container.appendChild(titleElement);

    if (!data || data.length === 0) {
        container.innerHTML += '<p>No data available</p>';
        return;
    }

    const tableContainer = document.createElement('div');
    tableContainer.className = 'table-container';

    const table = document.createElement('table');
    table.className = 'triplets-table';

    const headerRow = document.createElement('tr');
    headerRow.innerHTML = `
        <th>Artist 1</th>
        <th>Artist 2</th>
        <th>Artist 3</th>
        <th>Total Users</th>
    `;
    table.appendChild(headerRow);

    data.forEach((item, index) => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${truncateText(item.artist1, 30)}</td>
            <td>${truncateText(item.artist2, 30)}</td>
            <td>${truncateText(item.artist3, 30)}</td>
            <td>${item.total_users.toLocaleString()}</td>
        `;
        table.appendChild(row);
    });

    tableContainer.appendChild(table);
    container.appendChild(tableContainer);
}

// Helper function to truncate long text
function truncateText(text, maxLength) {
    if (text.length > maxLength) {
        return text.substring(0, maxLength) + '...';
    }
    return text;
}

function getColor(index) {
    const colors = [
        '#A8DADC', '#457B9D', '#E63946', '#F4A261', '#2A9D8F',
        '#9B5DE5', '#F15BB5', '#FEE440', '#00BBF9', '#00F5D4',
        '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7',
        '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9',
        '#FF9F1C', '#E71D36', '#2EC4B6', '#FDFFFC', '#011627'
    ];
    return colors[index % colors.length];
}