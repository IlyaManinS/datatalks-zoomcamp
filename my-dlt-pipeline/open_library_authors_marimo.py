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
    # Connect to the DuckDB database created by the open_library_pipeline.
    # Use the local file in read-only mode for exploration.
    con = ibis.duckdb.connect(database="open_library_pipeline.duckdb", read_only=True)
    return con


@app.cell
def _(con):
    # Access the main books table and the authors child table via SQL.
    books = con.sql("SELECT * FROM open_library_pipeline_dataset.books")
    authors = con.sql("SELECT * FROM open_library_pipeline_dataset.books__authors")
    return authors, books


@app.cell
def _(authors, ibis):
    # Compute top 10 authors by number of distinct books.
    top_authors = (
        authors.group_by(authors.name)
        .aggregate(book_count=authors._dlt_parent_id.nunique())
        .order_by(ibis.desc("book_count"))
        .limit(10)
    )
    top_authors_df = top_authors.execute()
    return top_authors_df


@app.cell
def _(alt, top_authors_df):
    # Simple bar chart of top 10 authors by book count.
    chart = (
        alt.Chart(top_authors_df)
        .mark_bar()
        .encode(
            x=alt.X("book_count:Q", title="Number of Books"),
            y=alt.Y("name:N", sort="-x", title="Author"),
            tooltip=["name", "book_count"],
        )
        .properties(title="Top 10 authors by book count")
    )
    chart
    return chart


if __name__ == "__main__":
    app.run()

