def imports_exports (
    year:int = None, #or string
    region: str = None,
    potential_trade_flows: dict = potential_imports,
    status: str = None # can be import or export

)-> dict:

    if status == "import":
        potential_trade_flows[year][status][region]=demand[year][region]-production_dict[year][region]

    else: # status == 'export'
        potential_trade_flows[year][staus][region]=production_dict[year][region]-demand[year][region]


    return potential_trade_flows

