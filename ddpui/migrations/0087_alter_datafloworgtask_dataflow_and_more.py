# Generated by Django 4.2 on 2024-07-25 06:54

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0086_alter_syncstats_sync_data_volume_b_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="datafloworgtask",
            name="dataflow",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="datafloworgtasks",
                to="ddpui.orgdataflowv1",
            ),
        ),
        migrations.AlterField(
            model_name="datafloworgtask",
            name="orgtask",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="orgtaskdataflows",
                to="ddpui.orgtask",
            ),
        ),
        migrations.AlterField(
            model_name="orgtnc",
            name="org",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="orgtncs",
                to="ddpui.org",
            ),
        ),
        migrations.AlterField(
            model_name="rolepermission",
            name="role",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="rolepermissions",
                to="ddpui.role",
            ),
        ),
        migrations.AlterField(
            model_name="tasklock",
            name="orgtask",
            field=models.OneToOneField(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="tasklock",
                to="ddpui.orgtask",
            ),
        ),
    ]
