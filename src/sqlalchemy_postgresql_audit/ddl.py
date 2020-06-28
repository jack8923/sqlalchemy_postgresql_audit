from .templates import make_audit_procedure, make_drop_audit_procedure


def get_audit_spec(table):
    audit_spec = table.info.get("audit.options", {"enabled": False})

    audit_spec["schema"] = audit_spec.get("schema_name", table.schema)
    audit_spec["session_settings"] = audit_spec.get("session_settings", [])

    return audit_spec


def get_create_transaction_trigger_ddl(
        target_columns,
        audit_columns,
        function_name,
        trigger_name,
        table_full_name,
        audit_table_full_name,
        session_settings=None,
):
    session_settings = session_settings or []

    deletion_elements = ["'D'", "now()", "current_user"]

    updation_elements = ["'U'", "now()", "current_user"]

    insertion_elements = ["'I'", "now()", "current_user"]

    setting_map = {
        session_setting.name: session_setting for session_setting in session_settings
    }

    column_elements = []
    check_settings = []

    for col in audit_columns.values():
        # We need to make sure to explicitly reference all elements in the procedure
        if col.name == 'id':
            continue

        column_elements.append(col.name)

        # To add session_settings (if any).
        if col.name in session_settings:
            session_setting = setting_map[col.name]
            type_str = session_setting.type.compile()
            name = session_setting.name.split("audit_", 1)[-1]

            session_settings_element = "current_setting('audit.{}', {})::{}".format(
                name, "true" if session_setting.nullable else "false", type_str
            )
            deletion_elements.append(session_settings_element)
            updation_elements.append(session_settings_element)
            insertion_elements.append(session_settings_element)

            # This handles a kind of strange behavior where if you set a session setting
            # and then commit the transaction you will end up with an empty string in that setting
            # and then the procedure will succeed, despite the value being "empty".
            if not session_setting.nullable:
                check_settings.append(
                    "IF {}::VARCHAR = '' THEN RAISE EXCEPTION "
                    "'audit.{} session setting must be set to a non null/empty value'; "
                    "END IF;".format(session_settings_element, name)
                )

    # Column to add 'id' of the table which was transacted
    deletion_elements.append("TG_TABLE_NAME")
    updation_elements.append("TG_TABLE_NAME")
    insertion_elements.append("TG_TABLE_NAME")

    return make_audit_procedure(
        audit_table_full_name=audit_table_full_name,
        table_full_name=table_full_name,
        procedure_name=function_name,
        trigger_name=trigger_name,
        deletion_elements=deletion_elements,
        updation_elements=updation_elements,
        insertion_elements=insertion_elements,
        audit_columns=column_elements,
        check_settings=check_settings,
    )


def get_create_data_trigger_ddl(
        target_columns,
        audit_columns,
        function_name,
        trigger_name,
        table_full_name,
        audit_table_full_name,
):
    deletion_elements = []

    updation_elements = []

    insertion_elements = []

    column_elements = []
    check_settings = []

    column_elements.append('data')

    # To add JSON data
    deletion_elements.append("to_json(OLD)")
    updation_elements.append("to_json(NEW)")
    insertion_elements.append("to_json(NEW)")

    return make_audit_procedure(
        audit_table_full_name=audit_table_full_name,
        table_full_name=table_full_name,
        procedure_name=function_name,
        trigger_name=trigger_name,
        deletion_elements=deletion_elements,
        updation_elements=updation_elements,
        insertion_elements=insertion_elements,
        audit_columns=column_elements,
        check_settings=check_settings,
    )


def get_drop_trigger_ddl(function_name, trigger_name, table_full_name):
    return make_drop_audit_procedure(function_name, trigger_name, table_full_name)


def install_audit_triggers(metadata, engine=None):
    """Installs all audit triggers.

    This can be used after calling `metadata.create_all()` to create
    all the procedures and triggers.

    :param metadata: A :class:`sqlalchemy.sql.schema.MetaData`
    :param engine: A :class:`sqlalchemy.engine.Engine` or None
    :return: None or a :class:`str` for the DDL needed to install all audit triggers.
    """
    audit_table_ddl = [
        t.info["audit.create_ddl"]
        for t in metadata.tables.values()
        if t.info.get("audit.is_audited")
    ]

    engine = engine or metadata.bind

    if engine:
        for ddl in audit_table_ddl:
            engine.execute(ddl[0])
            engine.execute(ddl[1])
    else:
        return "; ".join(audit_table_ddl)


def uninstall_audit_triggers(metadata, engine=None):
    """Uninstalls all audit triggers.

    This can be used to remove all audit triggers.

    :param metadata: A :class:`sqlalchemy.sql.schema.MetaData`
    :param engine: A :class:`sqlalchemy.engine.Engine` or None
    :return: None or a :class:`str` for the DDL needed to uninstall all audit triggers.
    """
    audit_table_ddl = [
        t.info["audit.drop_ddl"]
        for t in metadata.tables.values()
        if t.info.get("audit.is_audited")
    ]

    engine = engine or metadata.bind

    if engine:
        for ddl in audit_table_ddl:
            engine.execute(ddl[0])
            engine.execute(ddl[1])
    else:
        return ";\n ".join(audit_table_ddl)
