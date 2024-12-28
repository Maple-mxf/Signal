package signal.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SignalException extends RuntimeException {
    private final Set<String> errorLabels = new HashSet<>(2, 0.8F);

    public SignalException() {
    }

    public SignalException(String message) {
        super(message);
    }

    public SignalException(String message, String... errorLabels) {
        super(message);
        this.addLabels(errorLabels);
    }

    @Deprecated
    public void addLabel(final String errorLabel) {
        errorLabels.add(errorLabel);
    }

    public void addLabels(String... errorLabels) {
        this.errorLabels.addAll(Arrays.asList(errorLabels));
    }

    public Set<String> getErrorLabels() {
        return Collections.unmodifiableSet(errorLabels);
    }

    public boolean hasErrorLabel(final String errorLabel) {
        return errorLabels.contains(errorLabel);
    }

    @Deprecated
    public static SignalException withErrorLabel(String... labels) {
        SignalException error = new SignalException();
        for (String label : labels) {
            error.addLabel(label);
        }
        return error;
    }


//    public static SignalException newError(String message, String... errorLabels) {
//        SignalException error = new SignalException(message);
//        if (errorLabels == null)
//            return error;
//        for (String errorLabel : errorLabels)
//            error.addLabel(errorLabel);
//        return error;
//    }


}
