def generate_emissivity_charts (df: pd.DataFrame, year: int = None, region: str = None, scope: str= None, filepath: str = None):
    """generates bar chart with emissions [t CO2/ t steel] per technology. Displays scope1, scope2, scope2 or combination of scopes

    Args:
        df (pd.DataFrame): calculated_emissivity_combined_
        year (int, optional): _description_. Defaults to None.
        region (str, optional): _description_. Defaults to None.
        scope (str, optional): _description_. Defaults to None.
        filepath (str, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    region_list= region
    if not region:
        region_list = ['Global']
        filename = f'emissions_chart {scope}'
    filename = f'emissions_chart {scope}'
    region_list = ', '.join(region_list)
    
    
    df_c=df.copy()
    df_c=df_c.groupby(['technology', 'year', 'region'], as_index=False).agg({'s1_emissivity': np.mean,
                                                                        's2_emissivity': np.mean,
                                                                        's3_emissivity': np.mean,
                                                                        'combined_emissivity': np.mean})
    df_c=pd.melt(df_c, id_vars=['year', 'region', 'technology'], value_vars=['s1_emissivity','s2_emissivity','s3_emissivity','combined_emissivity'], var_name='metric')
    print(df_c)
    sorter=["Avg BF-BOF","BAT BF-BOF","DRI-EAF",
    "BAT BF-BOF_H2 PCI","BAT BF-BOF_bio PCI","DRI-EAF_50% bio-CH4","DRI-EAF_50% green H2","DRI-Melt-BOF","Smelting Reduction",
    "BAT BF-BOF+CCUS","BAT BF-BOF+CCU","BAT BF-BOF+BECCUS","DRI-EAF+CCUS","DRI-EAF_100% green H2","DRI-Melt-BOF+CCUS","DRI-Melt-BOF_100% zero-C H2","Electrolyzer-EAF","Electrowinning-EAF","Smelting Reduction+CCUS",
    "EAF"]
    sorterIndex = dict(zip(sorter, range(len(sorter))))
        
    
    if scope == 's1_emissivity':
        df_c =df_c.loc[(df_c['region']==(region))& (df_c['year']== year)] #Note: Scope 1 emissivity only depends on the technology, not on the region
        print(df_c)
        df_c=df_c.loc[(df_c['metric']==scope)]
        print(df_c)
        df_c['tech_order']=df_c['technology'].map(sorterIndex)
        df_c.sort_values(['tech_order'], ascending=True, inplace=True)
        df_c.drop('tech_order',1, inplace=True)
        
        t=f'{scope}, in {year}'
        c= 'technology'
        
    elif scope == 's2_emissivity':
        df_c =df_c.loc[(df_c['region']==(region))& (df_c['year']== year)] #Note: Scope 1 emissivity only depends on the technology, not on the region
        print(df_c)
        df_c=df_c.loc[(df_c['metric']==scope)]
        print(df_c)
        df_c['tech_order']=df_c['technology'].map(sorterIndex)
        df_c.sort_values(['tech_order'], ascending=True, inplace=True)
        df_c.drop('tech_order',1, inplace=True)
        
        t=f'{scope}, in {year}'
        c= 'technology'
        
    elif scope == 's3_emissivity':
        df_c =df_c.loc[(df_c['region']==(region))& (df_c['year']== year)] #Note: Scope 1 emissivity only depends on the technology, not on the region
        print(df_c)
        df_c=df_c.loc[(df_c['metric']==scope)]
        print(df_c)
        df_c['tech_order']=df_c['technology'].map(sorterIndex)
        df_c.sort_values(['tech_order'], ascending=True, inplace=True)
        df_c.drop('tech_order',1, inplace=True)
        
        t=f'{scope}, in {year}'
        c= 'technology'
        
    elif scope== 's1+s2' :
        df_c =df_c.loc[(df_c['region']== region)& (df_c['year']==(year))] #Note: Scope 2 emissivity depends on the technolog and region
        print(df_c)
        df_c=df_c.loc[(df_c['metric']=='s1_emissivity')|(df_c['metric']=='s2_emissivity')]
        print(df_c)
        df_c['tech_order']=df_c['technology'].map(sorterIndex)
        df_c.sort_values(['tech_order'], ascending=True, inplace=True)
        df_c.drop('tech_order',1, inplace=True)
        print(df_c)
        t=f'{scope}, in {region}, in {year}'
        c='metric'
        
    elif scope == 'combined':
        df_c =df_c.loc[(df_c['region']== region)& (df_c['year']==(year))] #Note: Scope 2 emissivity depends on the technolog and region
        print(df_c)
        df_c=df_c.loc[(df_c['metric']=='s1_emissivity')|(df_c['metric']=='s2_emissivity')|(df_c['metric']=='s3_emissivity')]
        print(df_c)
        df_c['tech_order']=df_c['technology'].map(sorterIndex)
        df_c.sort_values(['tech_order'], ascending=True, inplace=True)
        df_c.drop('tech_order',1, inplace=True)
        print(df_c)
        t=f'{scope}, in {region}, in {year}'
        c='metric'
        
    fig_= px.bar(
        df_c,
        x='technology',
        y='value',
        color= c,
        text_auto='.2f',
        labels={'value': '[t CO2/t steel]'},
        title= t
        
    )
    if filepath:
        filename = f'{filepath}/{filename}'
    return fig_