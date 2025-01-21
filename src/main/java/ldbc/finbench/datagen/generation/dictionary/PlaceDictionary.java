/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ldbc.finbench.datagen.generation.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import ldbc.finbench.datagen.entities.place.Place;
import ldbc.finbench.datagen.generation.DatagenParams;
import ldbc.finbench.datagen.util.ZOrder;

/**
 * This class reads the files containing the country data and city data used in the LDBC social network generation and
 * provides access methods to the data.
 * Most of the persons has the prerequisite of requiring a valid location id.
 */
public class PlaceDictionary {

    static final int INVALID_LOCATION = -1;
    private static final String SEPARATOR = " ";
    private static final String SEPARATOR_CITY = " ";

    private PlaceZOrder[] sortedPlace;
    private Float[] cumulativeDistribution;

    private List<Integer> countries;

    private Map<Integer, Place> places;

    private Map<Integer, Integer> isPartOf;

    private Map<Integer, List<Integer>> citiesByCountry;

    private Map<String, Integer> cityNames;

    private Map<String, Integer> countryNames;

    public PlaceDictionary() {
        this.countryNames = new HashMap<>();
        this.cityNames = new HashMap<>();
        this.places = new HashMap<>();
        this.isPartOf = new HashMap<>();
        this.countries = new ArrayList<>();
        this.citiesByCountry = new HashMap<>();
        load();
    }

    public Set<Integer> getPlaces() {
        return places.keySet();
    }

    public List<Integer> getCountries() {
        return new ArrayList<>(countries);
    }

    public String getPlaceName(int placeId) {
        return places.get(placeId).getName();
    }

    String getType(int placeId) {
        return places.get(placeId).getType();
    }

    public int getCityId(String cityName) {
        if (!cityNames.containsKey(cityName)) {
            return INVALID_LOCATION;
        }
        return cityNames.get(cityName);
    }

    public int getCountryId(String countryName) {
        if (!countryNames.containsKey(countryName)) {
            return INVALID_LOCATION;
        }
        return countryNames.get(countryName);
    }

    public int belongsTo(int placeId) {
        if (!isPartOf.containsKey(placeId)) {
            return INVALID_LOCATION;
        }
        return isPartOf.get(placeId);
    }

    public int getRandomCity(Random random, int countryId) {
        if (!citiesByCountry.containsKey(countryId)) {
            System.out.println("Invalid countryId");
            return INVALID_LOCATION;
        }
        if (citiesByCountry.get(countryId).size() == 0) {
            Place placeId = places.get(countryId);
            String countryName = placeId.getName();
            System.out.println("Country with no known cities: " + countryName);
            return INVALID_LOCATION;
        }

        int randomNumber = random.nextInt(citiesByCountry.get(countryId).size());
        return citiesByCountry.get(countryId).get(randomNumber);
    }

    public int getRandomCountryUniform(Random random) {
        int randomNumber = random.nextInt(countries.size());
        return countries.get(randomNumber);
    }

    private void load() {
        readCountries(DatagenParams.countryDictionaryFile);
        orderByZ();
        readCities(DatagenParams.cityDictionaryFile);
        readContinents(DatagenParams.countryDictionaryFile);
    }

    private void readCities(String fileName) {
        try {
            BufferedReader dictionary =
                new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            String line;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(SEPARATOR_CITY);
                if (countryNames.containsKey(data[0])) {
                    Integer countryId = countryNames.get(data[0]);
                    if (!cityNames.containsKey(data[1])) {
                        Place placeId = new Place();
                        placeId.setId(places.size());
                        placeId.setName(data[1]);
                        placeId.setLatitude(places.get(countryId).getLatitude());
                        placeId.setLongitude(places.get(countryId).getLongitude());
                        placeId.setPopulation(-1);
                        placeId.setType(Place.CITY);

                        places.put(placeId.getId(), placeId);
                        isPartOf.put(placeId.getId(), countryId);
                        citiesByCountry.get(countryId).add(placeId.getId());

                        cityNames.put(data[1], placeId.getId());
                    }
                }
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void readCountries(String fileName) {
        try {
            BufferedReader dictionary =
                new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            List<Float> temporalCumulative = new ArrayList<>();

            String line;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(SEPARATOR);
                String placeName = data[1];

                Place place = new Place();
                place.setId(places.size());
                place.setName(placeName);
                place.setLatitude(Double.parseDouble(data[2]));
                place.setLongitude(Double.parseDouble(data[3]));
                place.setPopulation(Integer.parseInt(data[4]));
                place.setType(Place.COUNTRY);

                places.put(place.getId(), place);
                countryNames.put(placeName, place.getId());
                float dist = Float.parseFloat(data[5]);
                temporalCumulative.add(dist);
                countries.add(place.getId());

                citiesByCountry.put(place.getId(), new ArrayList<>());
            }
            dictionary.close();
            cumulativeDistribution = new Float[temporalCumulative.size()];
            cumulativeDistribution = temporalCumulative.toArray(cumulativeDistribution);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void readContinents(String fileName) {
        Map<String, Integer> treatedContinents = new HashMap<>();
        try {
            BufferedReader dictionary =
                new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            String line;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(SEPARATOR);
                String placeName = data[1];

                int countryId = countryNames.get(placeName);

                if (!treatedContinents.containsKey(data[0])) {

                    Place continent = new Place();
                    continent.setId(places.size());
                    continent.setName(data[0]);
                    continent.setLatitude(Double.parseDouble(data[2]));
                    continent.setLongitude(Double.parseDouble(data[3]));
                    continent.setPopulation(0);
                    continent.setType(Place.CONTINENT);

                    places.put(continent.getId(), continent);
                    treatedContinents.put(data[0], continent.getId());
                }
                Integer continentId = treatedContinents.get(data[0]);
                long population = places.get(continentId).getPopulation() + places.get(countryId).getPopulation();
                places.get(continentId).setPopulation(population);
                isPartOf.put(countryId, continentId);
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getCountryForPerson(Random random) {
        int position = Arrays.binarySearch(cumulativeDistribution, random.nextFloat());
        if (position >= 0) {
            return position;
        }
        return (-(position + 1));
    }

    public float getCumProbabilityCountry(int countryId) {
        if (countryId == 0) {
            return cumulativeDistribution[0];
        }
        return cumulativeDistribution[countryId] - cumulativeDistribution[countryId - 1];
    }

    private void orderByZ() {
        ZOrder zorder = new ZOrder(8);
        sortedPlace = new PlaceZOrder[countries.size()];

        for (int i = 0; i < countries.size(); i++) {
            Place loc = places.get(countries.get(i));
            int zvalue = zorder.getZValue(((int) Math.round(loc.getLongitude()) + 180) / 2,
                                          ((int) Math.round(loc.getLatitude()) + 180) / 2);
            sortedPlace[i] = new PlaceZOrder(loc.getId(), zvalue);
        }

        Arrays.sort(sortedPlace);
        for (int i = 0; i < sortedPlace.length; i++) {
            places.get(sortedPlace[i].id).setZId(i);
        }
    }

    public int getZorderID(int placeId) {
        return places.get(placeId).getZId();
    }

    public int getPlaceIdFromZOrder(int zorderId) {
        return sortedPlace[zorderId].id;
    }

    public Place getLocation(int id) {
        return places.get(id);
    }
}
