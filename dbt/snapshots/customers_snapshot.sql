{% snapshot customers_snapshot %}
{{
    config(
      target_schema='snapshots',
      unique_key='customer_sk',
      strategy='check',
      check_cols=['email', 'country', 'status']
    )
}}

select * from {{ ref('customer_360') }}

{% endsnapshot %}
