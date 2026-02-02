@step(trigger_rule="none_failed_min_one_success")
def zip_all_results(
    directory_path: str,
    mode_info: Dict[str, Any],
    **kwargs
):
    ti = kwargs["ti"]

    # Récupération des XComs des mapped tasks
    results = ti.xcom_pull(
        task_ids="extract_iam_for_account",
        map_indexes=None
    ) or []

    # results contient UNIQUEMENT les succès
    successful_results = [r for r in results if r]

    successful = len(successful_results)

    # Nombre total de comptes
    accounts = ti.xcom_pull(task_ids="get_accounts_list") or []
    total_accounts = len(accounts)

    failed = total_accounts - successful



extraction_tasks = extract_iam_for_account.expand(
    account=accounts
).partial(
    directory_path=directory,
    max_active_tis_per_dagrun=10
)

compression_summary = zip_all_results(
    directory_path=directory,
    mode_info=mode_info
)



3########


@step(trigger_rule="all_done")
def extraction_barrier():
    """
    Task technique qui sert uniquement à :
    - attendre la fin de toutes les mapped tasks
    - NE PAS propager l'état FAILED
    """
    pass


extraction >> extraction_barrier

compression_summary = zip_all_results(
    directory_path=directory,
    mode_info=mode_info
)

extraction_barrier >> compression_summary
