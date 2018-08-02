package example_java.common;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JavaData {

    public static List<Person> sampleData() throws IOException {
        return sampleDataAsStrings().stream().map(line -> {
            String[] parts = line.split(",");
            return new Person(parts[0], parts[1], getAge(parts[2]), parts[3]);
        }).collect(Collectors.toList());
    }

    public static List<String> sampleDataAsStrings() throws IOException {
        List list = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(new File("src/main/resources/sample_data.csv")));
        String line;
        while ((line=br.readLine())!=null) {
            list.add(line);
        }
        br.close();
        return list;
    }


    private static int getAge(String ageVal){
        if (StringUtils.isNotEmpty(ageVal)) {
            return Integer.parseInt(ageVal);
        }
        return 0;

    }
}
