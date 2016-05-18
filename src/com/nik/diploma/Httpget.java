package com.nik.diploma;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by nik on 02.03.16.
 */
public class Httpget {

    public enum kekus {
        EGAA,EGAB,EGAC,EGAD,EGAE,EGAH,EGAL,EGAR,EGBB,EGBC,EGBD,EGBE,EGBF,EGBG,EGBJ,EGBK,EGBL,EGBM,EGBN,EGBO,EGBP,EGBR,EGBS,EGBT,EGBV,EGBW,EGCA,EGCB,EGCC,EGCD,EGCE,EGCF,EGCG,EGCH,EGCJ,EGCK,EGCL,EGCN,EGCO,EGCP,EGCS,EGCT,EGCV,EGCW,EGDA,EGDC,EGDD,EGDJ,EGDL,EGDM,EGDN,EGDO,EGDP,EGDR,EGDT,EGDV,EGDW,EGDX,EGDY,EGEA,EGEC,EGED,EGEF,EGEG,EGEH,EGEL,EGEN,EGEO,EGEP,EGER,EGES,EGET,EGEW,EGEY,EGEZ,EGFA,EGFC,EGFE,EGFF,EGFH,EGFP,EGGD,EGGH,EGGP,EGGW,EGHA,EGHB,EGHC,EGHD,EGHE,EGHF,EGHG,EGHH,EGHI,EGHJ,EGHK,EGHL,EGHN,EGHO,EGHP,EGHQ,EGHR,EGHS,EGHT,EGHU,EGHY,EGJA,EGJB,EGJJ,EGKA,EGKB,EGKE,EGKG,EGKH,EGKK,EGKL,EGKM,EGKR,EGLA,EGLC,EGLD,EGLF,EGLG,EGLJ,EGLK,EGLL,EGLM,EGLP,EGLS,EGLT,EGLW,EGMA,EGMC,EGMD,EGMF,EGMH,EGMJ,EGMK,EGML,EGMT,EGNA,EGNB,EGNC,EGND,EGNE,EGNF,EGNG,EGNH,EGNI,EGNJ,EGNL,EGNM,EGNO,EGNP,EGNR,EGNS,EGNT,EGNU,EGNV,EGNW,EGNX,EGNY,EGOD,EGOE,EGOM,EGOQ,EGOS,EGOV,EGOW,EGOY,EGPA,EGPB,EGPC,EGPD,EGPE,EGPF,EGPG,EGPH,EGPI,EGPJ,EGPK,EGPL,EGPM,EGPN,EGPO,EGPR,EGPS,EGPT,EGPU,EGPW,EGPY,EGQB,EGQK,EGQL,EGQS,EGSA,EGSB,EGSC,EGSD,EGSF,EGSG,EGSH,EGSI,EGSJ,EGSK,EGSL,EGSM,EGSN,EGSO,EGSP,EGSQ,EGSR,EGSS,EGST,EGSU,EGSV,EGSW,EGSX,EGSY,EGTA,EGTB,EGTC,EGTD,EGTE,EGTF,EGTG,EGTH,EGTI,EGTK,EGTO,EGTP,EGTR,EGTU,EGTW,EGUA,EGUB,EGUD,EGUK,EGUL,EGUN,EGUO,EGUP,EGUT,EGUV,EGUW,EGUY,EGVA,EGVF,EGVG,EGVJ,EGVL,EGVN,EGVO,EGVP,EGVT,EGWA,EGWC,EGWE,EGWN,EGWU,EGWZ,EGXA,EGXB,EGXC,EGXD,EGXE,EGXG,EGXH,EGXJ,EGXN,EGXP,EGXQ,EGXS,EGXT,EGXU,EGXV,EGXW,EGXY,EGXZ,EGYC,EGYD,EGYE,EGYI,EGYM,EGYP,EGZJ,EGZL,EGZU,EGZV;

        public static String[] names() {
            kekus[] states = values();
            String[] names = new String[states.length];

            for (int i = 0; i < states.length; i++) {
                names[i] = states[i].name();
            }

            return names;
        }
    }
    public static void mane() throws Exception{
        PrintWriter writer = new PrintWriter("airport_weather1.csv", "UTF-8");

        for (String i : kekus.names()) {
            CSVParser webParser = CSVParser.parse(new URL("http://www.wunderground.com/history/airport/"+i+"/2014/1/1/CustomHistory.html?dayend=31&monthend=12&yearend=2014&format=1"), Charset.forName("UTF-8"), CSVFormat.RFC4180);
            //Iterator iter = webParser.iterator();
            for (CSVRecord csvRecord : webParser) {
                if (csvRecord.get(0).equalsIgnoreCase("GMT") || csvRecord.get(0).equalsIgnoreCase("")) {
                    System.out.println(csvRecord.toString());
                    continue;
                }
                String newLine = i;
                for (int j=0; j< csvRecord.size();j++) {
                    newLine = newLine + ",";
                    newLine = newLine + csvRecord.get(j);
                }
                writer.println(newLine);
            }
        }
        writer.close();
    }

}
