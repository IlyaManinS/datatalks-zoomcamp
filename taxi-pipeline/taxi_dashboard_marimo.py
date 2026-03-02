import marimo as mo


app = mo.App()


@app.cell
def _():
    import ibis

    return ibis


@app.cell
def _():
    import altair as alt

    return alt


@app.cell
def _(ibis):
    # Connect to the DuckDB database created by the taxi_pipeline.
    con = ibis.duckdb.connect(database="taxi_pipeline.duckdb", read_only=True)
    return con


@app.cell
def _(con):
    # Base table expression for the NYC taxi data.
    nyc_taxi = con.sql("SELECT * FROM taxi_data.nyc_taxi")
    return nyc_taxi


@app.cell
def _(con):
    # Daily aggregates for passengers and total amount.
    daily_passengers_amount = con.sql(
        """
        SELECT
          DATE(trip_pickup_date_time) AS trip_date,
          SUM(passenger_count)        AS total_passengers,
          SUM(total_amt)              AS total_amount
        FROM taxi_data.nyc_taxi
        GROUP BY trip_date
        ORDER BY trip_date
        """
    )
    daily_passengers_amount_df = daily_passengers_amount.execute()
    return daily_passengers_amount_df


@app.cell
def _(con):
    # Daily average trip distance.
    daily_avg_distance = con.sql(
        """
        SELECT
          DATE(trip_pickup_date_time) AS trip_date,
          AVG(trip_distance)          AS avg_trip_distance
        FROM taxi_data.nyc_taxi
        GROUP BY trip_date
        ORDER BY trip_date
        """
    )
    daily_avg_distance_df = daily_avg_distance.execute()
    return daily_avg_distance_df


@app.cell
def _(con):
    # Trip counts per payment type for the whole period.
    payment_type_counts = con.sql(
        """
        SELECT
          payment_type,
          COUNT(*) AS trips,
          COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS pct
        FROM taxi_data.nyc_taxi
        GROUP BY payment_type
        ORDER BY trips DESC
        """
    )
    payment_type_counts_df = payment_type_counts.execute()
    return payment_type_counts_df


@app.cell
def _(alt, daily_passengers_amount_df):
    # Line chart: daily total passengers and total amount (dual y-axis).
    _base_daily = alt.Chart(daily_passengers_amount_df).encode(
        x=alt.X("trip_date:T", title="Pickup date"),
    )

    passengers_line = (
        _base_daily.transform_calculate(series="'Total passengers'")
        .mark_line(color="steelblue")
        .encode(
        y=alt.Y(
            "total_passengers:Q",
            axis=alt.Axis(title="Total passengers"),
            ),
            color=alt.Color("series:N", title="Metric"),
        )
    )

    amount_line = (
        _base_daily.transform_calculate(series="'Total amount (USD)'")
        .mark_line(color="firebrick")
        .encode(
        y=alt.Y(
            "total_amount:Q",
            axis=alt.Axis(title="Total amount (USD)", orient="right"),
            ),
            color=alt.Color("series:N", title="Metric"),
        )
    )

    daily_passengers_amount_chart = (
        alt.layer(passengers_line, amount_line)
        .resolve_scale(y="independent")
        .properties(title="Daily passengers and total amount")
    )

    daily_passengers_amount_chart
    return daily_passengers_amount_chart


@app.cell
def _(alt, daily_avg_distance_df):
    # Bar chart: average trip distance per day.
    daily_avg_distance_chart = (
        alt.Chart(daily_avg_distance_df)
        .mark_bar()
        .encode(
            x=alt.X("trip_date:T", title="Pickup date"),
            y=alt.Y(
                "avg_trip_distance:Q",
                title="Average trip distance per trip",
            ),
            tooltip=[
                alt.Tooltip("trip_date:T", title="Pickup date"),
                alt.Tooltip(
                    "avg_trip_distance:Q",
                    title="Avg distance per trip",
                    format=".2f",
                ),
            ],
        )
        .properties(title="Average trip distance per day")
    )

    daily_avg_distance_chart
    return daily_avg_distance_chart


@app.cell
def _(alt, payment_type_counts_df):
    # Pie chart: share of trips by payment type for the whole period.
    _base_pie = alt.Chart(payment_type_counts_df).encode(
        theta=alt.Theta("trips:Q", title="Trips", stack=True, sort="descending"),
        color=alt.Color("payment_type:N", title="Payment type"),
        order=alt.Order("trips:Q", sort="descending"),
        tooltip=["payment_type", "trips"],
    )

    pie = _base_pie.mark_arc()

    # Percentage labels: use a separate chart so the legend from `pie` remains visible.
    # Position labels just outside the pie, and drop values that would round to 0.0%.
    labels = (
        alt.Chart(payment_type_counts_df)
        .transform_filter("datum.pct >= 0.0005")
        .mark_text(radius=110, radiusOffset=12, fontSize=12)
        .encode(
            theta=alt.Theta("trips:Q", stack=True, sort="descending"),
            order=alt.Order("trips:Q", sort="descending"),
            text=alt.Text("pct:Q", format=".1%"),
            color=alt.value("black"),
        )
    )

    payment_type_pie_chart = (
        (pie + labels).properties(title="Trip share by payment type")
    )

    payment_type_pie_chart
    return payment_type_pie_chart


if __name__ == "__main__":
    app.run()

