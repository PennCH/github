import mysql.connector
from decimal import Decimal
import datetime
import time
import xlrd
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import schedule
import logging

logger = logging.getLogger('checkDailyReport')

logging.basicConfig(filename = 'C:\checkDailyLog.log',
                    level = logging.INFO,
                    format = '%(asctime)s %(levelname)s %(message)s')

ROWSNUM = 0
#每次循环的邮件信息
context = ''
#全部邮件信息
fullContext = ''
#判断是否存在差异，如果存在差异True，则发送邮件，如果不存在差异则不发送邮件
isSendMail = False

def getDataBaseInfo():
    #根据路径获取excel信心
    #database = xlrd.open_workbook('F:\databasepzcatering.xlsx')
    database = xlrd.open_workbook('C:\\temp\\databasepzcatering.xlsx')
    #获取指定sheet内信息
    sheet = database.sheet_by_name('database')
    return sheet
    #print(sheet)
    #获取记录行数
    #ROWSNUM = SHEET.nrows
    #rows = SHEET.get_rows()
    #print(rows)
    #return rows

#rows = getDataBaseInfo()
#print(rows)

#如果有日报不平，则发送邮件

#def sendMail(HOST,DATABASE,processDay):
def sendMail(context):
    #mail_host='smtp.qq.com'
    #mail_user='120821615@qq.com'
    #需要通过邮箱设置获取授权码
    #mail_pass='eijxwykwvyddbibh'

    mail_host = 'smtp.163.com'
    mail_user = '0910bhqt@163.com'
    # 需要通过邮箱设置获取授权码
    mail_pass = '0a8x2mjtwmy'

    recivers = '360346034@qq.com'

    message = MIMEText(str(context), 'plain', 'utf-8')
    message['From'] = '0910bhqt@163.com'
    message['To'] = '360346034@qq.com'
    # message['From'] = Header('监测程序', 'utf-8')
    # message['To'] = Header('报表监测相关人员', 'utf-8')

    subject = '营业日报数据不平'
    message['Subject'] = Header(subject, 'utf-8')

    try:
        #需要使用ssl形式发送邮件
        #smtpObj = smtplib.SMTP_SSL()
        smtpObj = smtplib.SMTP()
        #smtpObj.connect(mail_host,265)
        smtpObj.connect(mail_host, 25)
        smtpObj.login(mail_user,mail_pass)
        smtpObj.sendmail(mail_user, recivers, message.as_string())
        print('邮件发送成功')
    except smtplib.SMTPHeloError:
        print(str(smtplib.SMTPHeloError))
        logging.error('邮件发送失败 执行日期:%s 异常信息：%s' % (datetime.date.today(),str(smtplib.SMTPHeloError)))
        print('邮件发送失败')
    except smtplib.SMTPRecipientsRefused:
        print(str(smtplib.SMTPRecipientsRefused))
        logging.error('邮件发送失败 执行日期:%s 异常信息：%s' % (datetime.date.today(), str(smtplib.SMTPRecipientsRefused)))
        print('邮件发送失败')
    except smtplib.SMTPSenderRefused:
        print(str(smtplib.SMTPSenderRefused))
        logging.error('邮件发送失败 执行日期:%s 异常信息：%s' % (datetime.date.today(), str(smtplib.SMTPSenderRefused)))
        print('邮件发送失败')
    except smtplib.SMTPDataError:
        print(str(smtplib.SMTPDataError))
        logging.error('邮件发送失败 执行日期:%s 异常信息：%s' % (datetime.date.today(), str(smtplib.SMTPDataError)))
        print('邮件发送失败')
    except smtplib.SMTPNotSupportedError:
        print(str(smtplib.SMTPNotSupportedError))
        logging.error('邮件发送失败 执行日期:%s 异常信息：%s' % (datetime.date.today(), str(smtplib.SMTPNotSupportedError)))
        print('邮件发送失败')


def executSql(HOST, PORT, USER, PASSWORD, DATABASE):
    diffamount = 0
    # region Description
    conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,database=DATABASE)
    cursor = conn.cursor();

    # cursor.execute('''CREATE TABLE `rept_dailycheck` (
    # `fdi_id`  int NOT NULL AUTO_INCREMENT COMMENT '自增主键' ,
    # `fdd_date`  date NOT NULL DEFAULT '1997-01-01' COMMENT '查询日期' ,
    # `fdi_brandid`  int NOT NULL DEFAULT 0 COMMENT '品牌ID' ,
    # `fdc_brandname`  varchar(50) NOT NULL DEFAULT '' COMMENT '品牌名称' ,
    # `fdm_freeamount`  decimal(16,6) NOT NULL DEFAULT 0.00 COMMENT '优免金额' ,
    # `fdm_realamount`  decimal(16,6) NOT NULL DEFAULT 0.00 COMMENT '实收金额' ,
    # `fdm_dishesamount`  decimal(16,6) NOT NULL DEFAULT 0.00 COMMENT '菜品金额 和非菜品合计' ,
    # `fdm_diffamount`  decimal(16,6) NOT NULL DEFAULT 0.00 COMMENT '差值' ,
    # `fdc_remark`  varchar(100) NOT NULL DEFAULT '' COMMENT '备注' ,
    # `fdd_datetime`  datetime NOT NULL DEFAULT '1997-01-01 00:00:01' COMMENT '操作时间' ,
    # PRIMARY KEY (`fdi_id`)
    # )
    # COMMENT='营业日报展示详情异常提醒'
    # ;
    #
    # ''')

    cursor.execute('''
    select fdi_brandid,fdc_brandname from sys_brand;
    ''')

    brands = cursor.fetchall()
    for brand in brands:

        # region Description
        brandId = brand[0]
        brandName = brand[1]

        # 菜品金额
        cursor.execute('''
            select 
            sum(fdm_realamount) as dishesamount 
            from rept_rc_day 
            where fdd_paydate=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
            and fdi_ognid in (select fdi_ognid from sys_organizations where fdi_brandid=%s)''' % brandId)

        dishesamount = cursor.fetchone()
        if isinstance(dishesamount[0], Decimal):
            dishesamount = dishesamount[0]
        else:
            dishesamount = 0

        # 营业外收入 茶位费 服务费
        cursor.execute('''
            SELECT
            sum(fdm_vouchernoconsume+fdm_noconsume+fdm_teaamount+fdm_serviceamount) amount 
            from rept_bill
            where fdd_paydate=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
            and fdi_ognid in (select fdi_ognid from sys_organizations where fdi_brandid=%s)''' % brandId)
        noconsume = cursor.fetchone()
        # 返回结果是元组
        # print(type(noconsume[0]))
        if isinstance(noconsume[0], Decimal):
            noconsume = noconsume[0]
        else:
            noconsume = 0

        # 优免项
        cursor.execute('''
            SELECT
            #赠送
            SUM(fdm_freeAmount) +
            #单品折扣
            SUM(fdm_dandiscountamount)+
            #餐盒费
            FORMAT(SUM(fdm_boxtotalamount),2) +
            #会员价优惠金额
            SUM(fdm_pricediff) +
            #特价优惠金额
            SUM(fdm_specialoffer) +
            #折扣
            SUM(if(fdi_dsid>0 and (fdi_dstype<=1 or fdi_dstype is null),fdm_discountamount,0)) +
            #满减优惠
            SUM(if(fdi_dsid>0 and fdi_dstype=2,fdm_discountamount,0)) +
            #抹零
            SUM((fdm_changeamount+fdm_percentchangeamount)) 
            FROM rept_bill 
            where fdd_paydate=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
            and fdi_ognid in (select fdi_ognid from sys_organizations where fdi_brandid=%s) and fdi_billstatus=1;''' % brandId)

        benefitamount = cursor.fetchone()

        if isinstance(benefitamount[0], float):
            #benefitamount = benefitamount[0]
            benefitamount = Decimal('{:.2f}'.format(Decimal(str(benefitamount[0]))))
        else:
            benefitamount = 0

        # 不计入实收的支付方式
        cursor.execute('''
            select 
            SUM(P.fdm_amount) as amounts
            FROM rept_billpay P
            where P.fdd_paydate=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
            and P.fdi_ognid in (select fdi_ognid from sys_organizations where fdi_brandid=%s) and P.fdb_validpay =1 and P.fdi_paymode=0;''' % brandId)
        freeamount = cursor.fetchone()

        if isinstance(freeamount[0], Decimal):
            freeamount = freeamount[0];
        else:
            freeamount = 0

        # 计入实收的支付方式
        cursor.execute('''
            select 
            SUM(P.fdm_amount) as amounts
            FROM rept_billpay P
            where P.fdd_paydate=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
            and P.fdi_ognid in (select fdi_ognid from sys_organizations where fdi_brandid=%s) and P.fdb_validpay =1 and P.fdi_paymode=1; ''' % brandId)
        payamount = cursor.fetchone()

        if isinstance(payamount[0], Decimal):
            payamount = payamount[0];
        else:
            payamount = 0

        dishesinfo = dishesamount + noconsume
        freeinfo = benefitamount + freeamount


        if dishesinfo > freeinfo + payamount:
            remark = '菜品金额大于优免和实收的合计'
            diffamount = dishesinfo - freeinfo - payamount
        elif dishesinfo < freeinfo + payamount:
            remark = '菜品金额小于优免和实收的合计'
            diffamount = freeinfo + payamount - dishesinfo

        processDate = datetime.datetime.now()+datetime.timedelta(days=-1)
        processDayTime =  processDate.strftime("%Y-%m-%d %H:%M:%S")
        processDay = processDate.strftime("%Y-%m-%d")

        # endregion
        if diffamount != 0:
            global isSendMail
            isSendMail = True
            global fullContext
            cursor.execute('''insert into rept_dailycheck (fdd_date,fdi_brandid,fdc_brandname,fdm_freeamount,fdm_realamount,fdm_dishesamount,fdm_diffamount,fdc_remark,fdd_datetime)
                values(DATE_SUB(CURDATE(),INTERVAL 1 DAY) ,%s,%s,%s,%s,%s,%s,%s,%s)''',
                           [brandId, brandName, freeinfo, payamount, dishesinfo, diffamount, remark,processDayTime])
            context = '主机名称: %s 数据库地址：%s 差异日期：%s 品牌名称：%s\n' %( HOST, DATABASE, processDay, brandName)
            fullContext = fullContext+context

    conn.commit()
    cursor.close()

    # endregion


def processTask():
    sheets = getDataBaseInfo()
    for i in range(1, sheets.nrows):
        row_values = sheets.row_values(i)

        HOST = row_values[1]
        PORT = int(row_values[2])
        USER = row_values[3]
        PASSWORD = row_values[4]
        DATABASE = row_values[5]
        executSql(HOST, PORT, USER, PASSWORD, DATABASE)
        if isSendMail:
            sendMail(fullContext)
            logging.info('邮件发送成功 执行日期:%s 存在数据异常信息' % (datetime.date.today()))
        else:
            logging.info('执行日期:%s 没有数据异常信息' % (datetime.date.today()))


def doFirst():
    schedule.every().day.at("08:00").do(processTask)
    while True:
        schedule.run_pending()
        time.sleep(30)
    #processTask()


if __name__ == "__main__":
    doFirst()
    #processTask()











