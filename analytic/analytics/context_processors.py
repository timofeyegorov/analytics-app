@app.context_processor
def utility_processor():
    def is_tuple(value) -> bool:
        return isinstance(value, tuple)

    def format_int(value: int) -> str:
        if pd.isna(value):
            return ""
        return f"{round(value):,}".replace(",", " ")

    def format_float1(value: float) -> str:
        return f"{value:,.1f}".replace(",", " ")

    def format_float2(value: float) -> str:
        return f"{value:,.2f}".replace(",", " ")

    def format_date(value: datetime) -> str:
        return value.strftime("%Y-%m-%d")

    def format_percent(value: float) -> str:
        if pd.isna(value):
            return ""
        return value

    def order_table(
        name: str, available: List[Dict[str, str]]
    ) -> Optional[Dict[str, str]]:
        names = list(map(lambda item: item.get("name"), available))
        if name in names:
            return {
                "num": names.index(name) + 1,
                "direction": available[names.index(name)].get("direction"),
            }

    def render_int(value: int) -> str:
        if pd.isna(value):
            return ""
        return value

    return dict(
        is_tuple=is_tuple,
        format_int=format_int,
        format_float1=format_float1,
        format_float2=format_float2,
        format_date=format_date,
        order_table=order_table,
        format_percent=format_percent,
        render_int=render_int,
    )
