from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("ddpui", "0171_orgwarehouse_org_unique"),
    ]

    operations = [
        migrations.AddField(
            model_name="orgwarehouse",
            name="dbt_profile_secret_block",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="warehouse_dbt_profile_secret_block",
                to="ddpui.orgprefectblockv1",
            ),
        ),
    ]
