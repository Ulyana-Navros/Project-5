
      
      
      
      
      
      
      
      
      --Шаг 1. Узнаем, когда была первая транзакция для каждого студента. Начиная с этой даты, мы будем собирать его баланс уроков.
      
with first_payments

as (select    user_id
    
            , date_trunc('day' , min(transaction_datetime)) as first_payment_date 
            
    from skyeng_db.payments
    
    where operation_name = 'Покупка уроков'
    
         and status_name = 'success'
         
    group by  user_id) 
    

        --Шаг 2. Соберем таблицу с датами за каждый календарный день 2016 года. 
, all_dates
as (
    select  distinct date_trunc('day', class_start_datetime) as dt
    from skyeng_db.classes
    where date_part('year', class_start_datetime) = 2016
    group by dt
   )
   
        --Шаг 3. Узнаем, за какие даты имеет смысл собирать баланс для каждого студента.
, all_dates_by_user
as (
    select user_id
         , dt
    from first_payments fp 
        join all_dates ad
        on ad.dt >= fp.first_payment_date
   )

        --Шаг 4. Найдем все изменения балансов, связанные с успешными транзакциями.
, payments_by_dates
as (
    select user_id
         , date_trunc ('day' ,transaction_datetime) as payment_date
         , sum(classes) as transaction_balance_change
    from skyeng_db.payments
    where status_name = 'success'
           and date_part('year', transaction_datetime) = 2016
    group by user_id
         , payment_date
   )

        --Шаг 5. Найдем баланс студентов, который сформирован только транзакциями.
, payments_by_dates_cumsum
as (
    select adbu.user_id
         , dt
         , (coalesce(transaction_balance_change, 0)) as transaction_balance_change
         , sum(coalesce(transaction_balance_change, 0)) over (partition by adbu.user_id order by dt) as transaction_balance_change_cs
    from all_dates_by_user adbu
        left join payments_by_dates pbd
        on adbu.user_id = pbd.user_id
        and adbu.dt = pbd.payment_date
   )

        --Шаг 6. Найдем изменения балансов из-за прохождения уроков. 
, classes_by_dates
as (
    select user_id
        , class_start_datetime::date as class_date
        , -1 * count(*) as classes_balance_change
    from skyeng_db.classes
    where class_status in ('success', 'failed_by_student')
        and class_type != 'trial'
        and date_part('year', class_start_datetime) = 2016
    group by user_id
        , class_date
   )

        --Шаг 7. Кумулятивная сумма количества пройденных уроков.
, classes_by_dates_cumsum
as (
    select adbu.user_id
        , dt
        , (coalesce(classes_balance_change, 0)) as classes_balance_change
        , sum(coalesce(classes_balance_change, 0)) over (partition by adbu.user_id order by dt) as classes_balance_change_cs
    from all_dates_by_user adbu
        left join classes_by_dates cbd
        on adbu.user_id = cbd.user_id
        and adbu.dt = cbd.class_date
   )

        --Шаг 8. Балансы каждого студента.
, balances
as (
    select pbdc.*
         , classes_balance_change
         , classes_balance_change_cs
         , classes_balance_change_cs + transaction_balance_change_cs as balance
    from payments_by_dates_cumsum pbdc
    join classes_by_dates_cumsum cbdc
    on pbdc.user_id = cbdc.user_id
        and pbdc.dt = cbdc.dt
   )

        -- Задание 1
        -- Выберите топ-1000 строк из CTE balances с сортировкой по user_id и dt. Посмотрите на изменения балансов студентов.
        -- Какие вопросы стоит задавать дата-инженерам и владельцам таблицы payments?
-- select *
-- from balances 
-- order by user_id , dt
-- limit 1000

-- ?? Студенты, у которых нет успешных оплат продолжают получать уроки, баланс уходит в минус.
-- ?? Дата/время окончания урока меньше даты/времени начала
-- ?? У успешных транзакций нет id.

        --Шаг 9. Посмотрим, как менялось общее количество уроков на балансах студентов.

select dt
    , sum(transaction_balance_change) as sum_transaction_balance_change
    , sum(transaction_balance_change_cs) as sum_transaction_balance_change_cs
    , sum(classes_balance_change) as sum_classes_balance_change
    , sum(classes_balance_change_cs) as sum_classes_balance_change_cs
    , sum(balance) as sum_balance
from balances
group by dt
order by dt
 

        -- Задание 2. Создайте визуализацию (линейную диаграмму) итогового результата. Какие выводы можно сделать из получившейся визуализации?
-- На графике можно увидеть сезонность покупки уроков, списаний с баланса. 
-- Болшой баланс уроков на конец года - с чем может быть связано? Возможно были акции или не успевают проходить или другие причины?
