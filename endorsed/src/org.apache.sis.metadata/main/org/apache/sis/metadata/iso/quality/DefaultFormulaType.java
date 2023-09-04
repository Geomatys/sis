package org.apache.sis.metadata.iso.quality;

import org.opengis.metadata.quality.BasicMeasure;
import org.opengis.metadata.quality.EvaluationMethod;
import org.opengis.metadata.quality.FormulaLanguage;
import org.opengis.metadata.quality.FormulaType;
import org.opengis.util.InternationalString;

/**
 * Description of the formula used for quality measure.
 * See the {@link FormulaType} GeoAPI interface for more details.
 *
 * <h2>Limitations</h2>
 * <ul>
 *   <li>Instances of this class are not synchronized for multi-threading.
 *       Synchronization, if needed, is caller's responsibility.</li>
 *   <li>Serialized objects of this class are not guaranteed to be compatible with future Apache SIS releases.
 *       Serialization support is appropriate for short term storage or RMI between applications running the
 *       same version of Apache SIS. For long term storage, use {@link org.apache.sis.xml.XML} instead.</li>
 * </ul>
 *
 * @author  Erwan Roussel (Geomatys)
 * @author  Martin Desruisseaux (Geomatys)
 * @version 4.0
 * @since   4.0
 */

public class DefaultFormulaType extends ISOMetadata implements FormulaType {
    /**
     * Serial number for inter-operability with different versions.
     */
    private static final long serialVersionUID = -8161495979294263308L;

    /**
     * Formula explanation.
     */
    @SuppressWarnings("serial")
    private InternationalString key;

    /**
     * Language in which the formula is expressed.
     */
    @SuppressWarnings("serial")
    private FormulaLanguage language;

    /**
     * Language version in which the formula is expressed.
     */
    @SuppressWarnings("serial")
    private InternationalString languageVersion;

    /**
     * Formula expression in the chosen language.
     */
    @SuppressWarnings("serial")
    private InternationalString mathematicalFormula;


    /**
     * Constructs an initially empty formula type.
     */
    public DefaultFormulaType() {
    }

    /**
     * Constructs a new instance initialized with the values from the specified metadata object.
     * This is a <em>shallow</em> copy constructor, because the other metadata contained in the
     * given object are not recursively copied.
     *
     * @param object  the metadata to copy values from, or {@code null} if none.
     *
     * @see #castOrCopy(FormulaType)
     */
    public DefaultFormulaType(FormulaType object) {
        super(object);
        this.key                    = object.getKey();
        this.language               = object.getLanguage();
        this.languageVersion        = object.getLanguageVersion();
        this.mathematicalFormula    = object.getMathematicalFormula();
    }

    /**
     * Returns a SIS metadata implementation with the values of the given arbitrary implementation.
     * This method performs the first applicable action in the following choices:
     *
     * <ul>
     *   <li>If the given object is {@code null}, then this method returns {@code null}.</li>
     *   <li>Otherwise if the given object is already an instance of
     *       {@code BasicMeasure}, then it is returned unchanged.</li>
     *   <li>Otherwise a new {@code BasicMeasure} instance is created using the
     *       {@linkplain #DefaultFormulaType(FormulaType) copy constructor} and returned.
     *       Note that this is a <em>shallow</em> copy operation, because the other
     *       metadata contained in the given object are not recursively copied.</li>
     * </ul>
     *
     * @param  object  the object to get as a SIS implementation, or {@code null} if none.
     * @return a SIS implementation containing the values of the given object (may be the
     *         given object itself), or {@code null} if the argument was null.
     */
    public static DefaultFormulaType castOrCopy(final FormulaType object) {
        if (object == null || object instanceof DefaultFormulaType) {
            return (DefaultFormulaType) object;
        }
        return new DefaultFormulaType(object);
    }


    /**
     * Returns the formula explanation.
     *
     * @return text description of formula explanation.
     */
    @Override
    public InternationalString getKey() { return key; }

    /**
     * Sets the formula explanation.
     *
     * @param newValue the new text description of formula explanation.
     */
    public void setKey(InternationalString newValue) {
        checkWritePermission(key);
        key = newValue;
    }

    /**
     * Returns the language in which the formula is expressed.
     *
     * @return formula language description.
     */
    @Override
    public FormulaLanguage getLanguage() { return language; }

    /**
     * Sets the language in which the formula is expressed.
     *
     * @param newValue the new formula language description.
     */
    public void setLanguage(FormulaLanguage newValue) {
        checkWritePermission(language);
        language = newValue;
    }

    /**
     * Returns the language version in which the formula is expressed.
     *
     * @return the language version.
     */
    @Override
    public InternationalString getLanguageVersion() { return languageVersion; }

    /**
     * Sets the language version in which the formula is expressed.
     *
     * @param newValue the new language version.
     */
    public void setLanguageVersion(InternationalString newValue) {
        checkWritePermission(languageVersion);
        languageVersion = newValue;
    }

    /**
     * Returns the formula expression in the chosen language.
     *
     * @return formula expression in the chosen language.
     */
    @Override
    public InternationalString getMathematicalFormula() { return mathematicalFormula; }

    /**
     * Sets the formula expression in the chosen language.
     *
     * @param newValue the new formula expression in the chosen language.
     */
    public void setMathematicalFormula(InternationalString newValue) {
        checkWritePermission(mathematicalFormula);
        mathematicalFormula = newValue;
    }

}
