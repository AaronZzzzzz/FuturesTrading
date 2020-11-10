import xml.etree.ElementTree as ET
import datetime

def rectangle(index, dt0, dt1, y0, y1, width, color):
    box_element = ET.Element('comp-{}'.format(str(index)))
    box_element.set('board_id', '0')
    box_element.set('class_id', 'drw_rect')
    box_element.set('dur_id', 'M5')
    box_element.set('flags', '1873776640')
    box_element.set('id', str(1227180296 + index))
    box_element.set('ins_id', 'rb主连')
    box_element.set('line_color', str(color))
    box_element.set('line_type', '0')
    box_element.set('line_width', str(width))
    box_element.set('text_color', str(color))

    pt0 = ET.Element('point-0')
    pt0.set('m_dt', dt0.strftime("%Y,%#m,%#d,%#H,%#M,%#S") + ',0')
    pt0.set('mark_color', str(color))
    pt0.set('mark_font', '-12,0,0,0,400,0,0,0,0,3,2,1,34,微软雅黑')
    pt0.set('mark_text', '')
    pt0.set('y', str(y0))

    pt1 = ET.Element('point-1')
    pt1.set('m_dt', dt1.strftime("%Y,%#m,%#d,%#H,%#M,%#S") + ',0')
    pt1.set('mark_color', str(color))
    pt1.set('mark_font', '-12,0,0,0,400,0,0,0,0,3,2,1,34,微软雅黑')
    pt1.set('mark_text', '')
    pt1.set('y', str(y1))

    box_element.append(pt0)
    box_element.append(pt1)
    return box_element, index + 1


def ind_vol1(index):
    ta_element = ET.Element('comp-{}'.format(str(index)))
    ta_element.set('board_id', '3')
    ta_element.set('class_id', 'ind_ta')
    ta_element.set('flags', '-2142191616')
    ta_element.set('id', '19010406')
    ta_element.set('insid_param', '')
    ta_element.set('insid_selector', '0')
    ta_element.set('ta_class_name', 'VOL')
    
    sub_input = ET.Element('input')
    sub_input.set('inPriceOHLCV', 'K')

    sub_sub_outReal = ET.Element('outReal')
    sub_sub_outReal.set('color', '16711935')
    sub_sub_outReal.set('color2', '16744576')
    sub_sub_outReal.set('width', '1')

    sub_sub_outSignal = ET.Element('outSignal')
    sub_sub_outSignal.set('color', '65280')
    sub_sub_outSignal.set('color2', '33023')
    sub_sub_outSignal.set('width', '1')

    sub_output = ET.Element('output')
    sub_output.append(sub_sub_outReal)
    sub_output.append(sub_sub_outSignal)

    ta_element.append(sub_input)
    ta_element.append(sub_output)

    return ta_element, index + 1

def ind_vol2(index):
    ta_element = ET.Element('comp-{}'.format(str(index)))
    ta_element.set('board_id', '1')
    ta_element.set('class_id', 'ind_ta')
    ta_element.set('flags', '-2142203904')
    ta_element.set('id', '261484859')
    ta_element.set('insid_param', '')
    ta_element.set('insid_selector', '0')
    ta_element.set('ta_class_name', 'VOL')
    
    sub_input = ET.Element('input')
    sub_input.set('inPriceOHLCV', '')

    sub_sub_outReal = ET.Element('outReal')
    sub_sub_outReal.set('color', '16711935')
    sub_sub_outReal.set('color2', '8421376')
    sub_sub_outReal.set('width', '1')

    sub_sub_outSignal = ET.Element('outSignal')
    sub_sub_outSignal.set('color', '65535')
    sub_sub_outSignal.set('color2', '8388736')
    sub_sub_outSignal.set('width', '1')

    sub_output = ET.Element('output')
    sub_output.append(sub_sub_outReal)
    sub_output.append(sub_sub_outSignal)

    ta_element.append(sub_input)
    ta_element.append(sub_output)

    return ta_element, index + 1

def ind_oi(index, id, flags, color1, color2, board_id):
    ta_element = ET.Element('comp-{}'.format(str(index)))
    ta_element.set('board_id', str(board_id))
    ta_element.set('class_id', 'ind_ta')
    ta_element.set('flags', str(flags))
    ta_element.set('id', str(id))
    ta_element.set('insid_param', '')
    ta_element.set('insid_selector', '0')
    ta_element.set('ta_class_name', 'OI')
    
    sub_input = ET.Element('input')
    sub_input.set('inPriceI', 'K')

    sub_sub_outReal = ET.Element('outReal')
    sub_sub_outReal.set('color', str(color1))
    sub_sub_outReal.set('color2', str(color2))
    sub_sub_outReal.set('width', '1')

    sub_output = ET.Element('output')
    sub_output.append(sub_sub_outReal)

    ta_element.append(sub_input)
    ta_element.append(sub_output)

    return ta_element, index + 1

def ind_ma(index, id, period, color1, color2):
    ta_element = ET.Element('comp-{}'.format(str(index)))
    ta_element.set('board_id', '0')
    ta_element.set('class_id', 'ind_ta')
    ta_element.set('flags', '-2142203904')
    ta_element.set('id', str(id))
    ta_element.set('insid_param', '')
    ta_element.set('insid_selector', '0')
    ta_element.set('ta_class_name', 'MA')
    
    sub_input = ET.Element('input')
    sub_input.set('inReal', 'K')

    sub_opt_input = ET.Element('opt_input')
    sub_opt_input.set('optInTimePeriod', str(period))
    sub_opt_input.set('optInMAType', '1')

    sub_sub_outReal = ET.Element('outReal')
    sub_sub_outReal.set('color', str(color1))
    sub_sub_outReal.set('color2', str(color2))
    sub_sub_outReal.set('width', '1')

    sub_output = ET.Element('output')
    sub_output.append(sub_sub_outReal)

    ta_element.append(sub_input)
    ta_element.append(sub_opt_input)
    ta_element.append(sub_output)

    return ta_element, index + 1


def ind_macd(index):
    ta_element = ET.Element('comp-{}'.format(str(index)))
    ta_element.set('board_id', '4')
    ta_element.set('class_id', 'ind_ta')
    ta_element.set('flags', '-2142203904')
    ta_element.set('id', '1312734437')
    ta_element.set('insid_param', '')
    ta_element.set('insid_selector', '0')
    ta_element.set('ta_class_name', 'MACD')
    
    sub_input = ET.Element('input')
    sub_input.set('inReal', 'K')

    sub_opt_input = ET.Element('opt_input')
    sub_opt_input.set('optInFastPeriod', '12')
    sub_opt_input.set('optInSlowPeriod', '26')
    sub_opt_input.set('optInSignalPeriod', '9')


    sub_sub_outMACD = ET.Element('outMACD')
    sub_sub_outMACD.set('color', '8421504')
    sub_sub_outMACD.set('color2', '33023')
    sub_sub_outMACD.set('width', '1')

    sub_sub_outMACDSignal = ET.Element('outMACDSignal')
    sub_sub_outMACDSignal.set('color', '33023')
    sub_sub_outMACDSignal.set('color2', '16711935')
    sub_sub_outMACDSignal.set('width', '1')

    sub_sub_outMACDHist = ET.Element('outMACDHist')
    sub_sub_outMACDHist.set('color', '16711935')
    sub_sub_outMACDHist.set('color2', '8421504')
    sub_sub_outMACDHist.set('width', '1')

    sub_output = ET.Element('output')
    sub_output.append(sub_sub_outMACD)
    sub_output.append(sub_sub_outMACDSignal)
    sub_output.append(sub_sub_outMACDHist)
    

    ta_element.append(sub_input)
    ta_element.append(sub_opt_input)
    ta_element.append(sub_output)

    return ta_element, index + 1

def misc(index):

    def misc_comp(index, id, flags, class_id):
        misc_element = ET.Element('comp-{}'.format(str(index)))    
        misc_element.set('id', str(id))
        misc_element.set('flags', str(flags))
        misc_element.set('class_id', class_id)
        misc_element.set('board_id', '0')
        misc_element.set('insid_selector', '0')
        misc_element.set('insid_param', '')
        return misc_element, index + 1

    misc1, index = misc_comp(index, 'ind_orders', -2146406400,  'ind_orders')
    misc2, index = misc_comp(index, 'ind_positions', -2146406400,  'ind_positions')
    misc3, index = misc_comp(index, 'main_inday', 1064960,  'main_inday')
    misc4, index = misc_comp(index, 'main_kline', 1052672,  'main_kline')
    misc5, index = misc_comp(index, 'main_tick', 1056768,  'main_tick')

    return [misc1, misc2, misc3, misc4, misc5], index


dt0 = datetime.datetime(2020, 11, 3, 9, 50, 0)
dt1 = datetime.datetime(2020, 11, 4, 9, 0, 0)
y0 = 3758
y1 = 3720


doc = ET.parse('C:/Users/Administrator/Desktop/aaa.xml')
root = doc.getroot()
root.getchildren().index(root.find('views'))

components = root.find('views').find('view-1').find('chart').find('components')

index = 0
recs = []
for i in trades.index:
    trade = trades.loc[i]
    rec, index = rectangle(index, trade['start'], trade['end'], trade['start_price'], trade['end_price'], 1, 443446)
    recs += [rec]
macd, index = ind_macd(index)
vol1, index = ind_vol1(index)
oi1, index = ind_oi(index, 19070981, -2142191616, 19392, 255, 3)
vol2, index = ind_vol2(index)
ma1, index = ind_ma(index, 90308775, 480, 8421504, 8421504)
ma2, index = ind_ma(index, 90321334, 240, 36090, 33023)
ma3, index = ind_ma(index, 90327496, 24, 16711935, 16711935)
ma4, index = ind_ma(index, 90333767, 960, 7522389, 65280)
oi2, index = ind_oi(index, 90343143, -2142203904, 8421504, 255, 1)
misc, index = misc(index)
to_append = recs + [macd, vol1, oi1, vol2, ma1, ma2, ma3, ma4, oi2] + misc

components.clear()
components.set('count', str(index))
_ = [components.append(e) for e in to_append]





from xml.dom import minidom
xmlstr = ET.tostring(root, 'utf-8')
xmlstr = minidom.parseString(xmlstr)
# xmlstr = minidom.parseString(xmlstr).toprettyxml(indent="\t")

xmlstr = '\n'.join([line for line in xmlstr.toprettyxml(indent='\t').split('\n') if line.strip()])
with open('C:/Users/Administrator/Desktop/abc.xml', "bw") as f:
    f.write(xmlstr.encode('utf-8'))
