# Generated by Django 4.2 on 2024-11-17 07:42

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0111_remove_orgpreferences_trial_end_date_and_more"),
    ]

    operations = [
        migrations.RenameField(
            model_name="userpreferences",
            old_name="llm_optin",
            new_name="disclaimer_shown",
        ),
    ]
